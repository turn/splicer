package com.turn.splicer;

import java.util.concurrent.LinkedBlockingQueue;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SplicerMain {

	private static final Logger LOG = LoggerFactory.getLogger(SplicerMain.class);

	private static final int PORT = 9000;

	private static final int NUM_TSDS = 10;
	private static final String TSD_HOST = "dwh-data012.atl1.turn.com";

	public static void main(String[] args) throws InterruptedException {

		for (int i=0; i<NUM_TSDS; i++) {
			String r = TSD_HOST + ":800" + i;
			if (HttpWorker.TSDMap.get(TSD_HOST) == null) {
				HttpWorker.TSDMap.put(TSD_HOST, new LinkedBlockingQueue<String>());
			}
			HttpWorker.TSDMap.get(TSD_HOST).put(r);
			LOG.info("Registering {}", r);
		}

		final Server server = new Server();

		Connector connector = new SelectChannelConnector();
		connector.setPort(PORT);
		server.addConnector(connector);

		QueuedThreadPool qtp = new QueuedThreadPool();
		qtp.setName("jetty-qt-pool");
		server.setThreadPool(qtp);

		ServletHandler servletHandler = new ServletHandler();
		servletHandler.addServletWithMapping(SplicerServlet.class.getName(), "/api/query");

		HandlerList handlers = new HandlerList();
		handlers.setHandlers(new Handler[]{
				servletHandler,
				new DefaultHandler()
		});
		server.setHandler(handlers);

		// register shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					server.stop();
				} catch (Exception e) {
					LOG.error("Jetty Shutdown Error", e);
				} finally {
					LOG.info("Shutdown Server...");
				}
			}
		}, "jetty-shutdown-hook"));

		// and start the server
		try {
			LOG.info("Starting web server at port=" + PORT);
			server.start();
			server.join();
		} catch (Exception e) {
			throw new RuntimeException("Could not start web server", e);
		}
	}
}
