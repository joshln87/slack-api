package allbegray.slack.rtm;

import android.util.Log;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import allbegray.slack.exception.SlackException;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okhttp3.ws.WebSocket;
import okhttp3.ws.WebSocketCall;
import okhttp3.ws.WebSocketListener;
import okio.Buffer;

import static okhttp3.ws.WebSocket.TEXT;

public class SlackRealTimeMessagingClient {

	private final static String TAG = "SlackRtmClient";

	private String webSocketUrl;
	private ProxyServerInfo proxyServerInfo;
	private WebSocket ws;
	private WebSocketCall wsCall;
	private Map<String, List<EventListener>> listeners = new HashMap<String, List<EventListener>>();
	private List<CloseListener> closeListeners = new ArrayList<CloseListener>();
	private List<FailureListener> failureListeners = new ArrayList<FailureListener>();
	private boolean stop;
	private ObjectMapper mapper;
	private Integer pingMillis;

	public SlackRealTimeMessagingClient(String webSocketUrl) {
		this(webSocketUrl, null, null, null);
	}

	public SlackRealTimeMessagingClient(String webSocketUrl, ObjectMapper mapper) {
		this(webSocketUrl, null, mapper, null);
	}

	public SlackRealTimeMessagingClient(String webSocketUrl, Integer pingMillis) {
		this(webSocketUrl, null, null, pingMillis);
	}

	public SlackRealTimeMessagingClient(String webSocketUrl, ProxyServerInfo proxyServerInfo, ObjectMapper mapper) {
		this(webSocketUrl, proxyServerInfo, mapper, null);
	}

	public SlackRealTimeMessagingClient(String webSocketUrl, ProxyServerInfo proxyServerInfo, ObjectMapper mapper, Integer pingMillis) {
		if (mapper == null) {
			mapper = new ObjectMapper();
		}
		if (pingMillis == null) {
			pingMillis = 3 * 1000;
		}
		this.webSocketUrl = webSocketUrl;
		this.proxyServerInfo = proxyServerInfo;
		this.mapper = mapper;
		this.pingMillis = pingMillis;
	}
	
	public void addListener(Event event, EventListener listener) {
		addListener(event.name().toLowerCase(), listener);
	}

	public void addListener(String event, EventListener listener) {
		List<EventListener> eventListeners = listeners.get(event);
		if (eventListeners == null) {
			eventListeners = new ArrayList<EventListener>();
			listeners.put(event, eventListeners);
		}
		eventListeners.add(listener);
	}

	public void addCloseListener(CloseListener listener) {
		closeListeners.add(listener);
	}

	public void addFailureListener(FailureListener listener) {
		failureListeners.add(listener);
	}

	public void close() {
		Log.i(TAG, "Slack RTM closing...");
		stop = true;
		if (ws != null) {
			try {
				ws.close(1000, "");
			} catch (Exception e) {
				// ignore
			}
		}
		if (wsCall != null) {
			wsCall.cancel();
		}
		Log.i(TAG, "Slack RTM closed.");
	}

	public boolean connect() {
		try {
			OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder()
					.connectTimeout(0, TimeUnit.MILLISECONDS)
					.readTimeout(0, TimeUnit.MILLISECONDS)
					.writeTimeout(0, TimeUnit.MILLISECONDS);

			if (proxyServerInfo != null) {
				Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(
						proxyServerInfo.getHost(), proxyServerInfo.getPort()));
				clientBuilder.proxy(proxy);
			}

			final Request request = new Request.Builder().url(webSocketUrl).build();
			final OkHttpClient client = clientBuilder.build();
			wsCall = WebSocketCall.create(client, request);
			wsCall.enqueue(new WebSocketListener() {
				@Override
				public void onOpen(WebSocket webSocket, Response response) {
					ws = webSocket;
					Log.i(TAG, "Connected to Slack RTM (Real Time Messaging) server : " + webSocketUrl);
				}

				@Override
				public void onMessage(ResponseBody responseBody) throws IOException {
					String message = null;
					if (responseBody.contentType() == TEXT) {
						message = responseBody.string();
					} else {
						throw new SlackException("Unknown payload type : " + responseBody.contentType(), new IllegalStateException());
					}
					responseBody.source().close();

					String type = null;
					JsonNode node = null;
					try {
						node = mapper.readTree(message);
						type = node.findPath("type").asText();
					} catch (Exception e) {
						Log.e(TAG, e.getMessage(), e);
					}

					if (!"pong".equals(type)) {
						Log.i(TAG, "Slack RTM message : " + message);
					}

					if (type != null) {
						List<EventListener> eventListeners = listeners.get(type);
						if (eventListeners != null && !eventListeners.isEmpty()) {
							for (EventListener listener : eventListeners) {
								listener.onMessage(node);
							}
						}
					}
				}

				@Override
				public void onPong(Buffer payload) {
				}

				@Override
				public void onClose(int code, String reason) {
					stop = true;
					if (closeListeners != null && !closeListeners.isEmpty()) {
						for (CloseListener listener : closeListeners) {
							listener.onClose();
						}
					}
				}

				@Override
				public void onFailure(IOException e, Response response) {
					stop = true;
					e.printStackTrace();
					if (failureListeners != null && !failureListeners.isEmpty()) {
						for (FailureListener listener : failureListeners) {
							listener.onFailure(new SlackException("websocket error", e));
						}
					}
				}
			});
			client.dispatcher().executorService().shutdown();

			await();

		} catch (Exception e) {
			close();
			throw new SlackException(e);
		}
		return true;
	}

	private long socketId = 0;

	private void ping() {
		ObjectNode pingMessage = mapper.createObjectNode();
		pingMessage.put("id", ++socketId);
		pingMessage.put("type", "ping");
		String pingJson = pingMessage.toString();
		if (ws != null) {
			try {
				ws.sendMessage(RequestBody.create(TEXT, pingJson));
				Log.d(TAG, "ping : " + pingJson);
			} catch (IOException e) {
				Log.d(TAG, "websocket closed before onclose event");
				close();
			}
		}
	}

	private void await() {
		Thread thread = new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					Thread.sleep(pingMillis);
					while (!stop) {
						ping();
						Thread.sleep(pingMillis);
					}
				} catch (Exception e) {
					throw new SlackException(e);
				}
			}
		});
		thread.start();
	}

}
