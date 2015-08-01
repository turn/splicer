package com.turn.splicer;

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
	private static final String TSD_HOST = "dwh-data012";

	public static void main(String[] args) throws InterruptedException {

		long d = System.currentTimeMillis();
		LOG.info("{}", d - (3600 * 1000 * 24 * 7));
		System.exit(0);

		for (int i=0; i<NUM_TSDS; i++) {
			String r = TSD_HOST + ":800" + i;
			HttpWorker.TSDs.put(r);
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
