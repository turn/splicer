/**
 * Copyright 2015-2016 The Splicer Query Engine Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.turn.splicer;

import com.turn.splicer.cache.JedisClient;

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

	// port on which the splicer is listening
	private static final int PORT = Config.get().getInt("splicer.port");

	// comma separated hosts on which the TSDs are running
	private static final String TSD_HOSTS = Config.get().getString("tsd.hosts");

	// start and end port for TSDs on data nodes. Start is inclusive, End is exclusive.
	private static final int TSD_START_PORT = Config.get().getInt("tsd.start.port");
	private static final int TSD_END_PORT = Config.get().getInt("tsd.end.port");

	private static final int TSD_QUERIES_PER_PORT = Config.get().getInt("tsd.queries.per.port", 1);

	public static void main(String[] args) throws InterruptedException {

		if (Config.get().getBoolean("tsd.connect.enable")) {
			String[] TSDs = TSD_HOSTS.split(",");
			for (int i = 0; i < TSDs.length; i++) {
				TSDs[i] = TSDs[i].trim();
			}

			for (String TSD : TSDs) {
				for (int port = TSD_START_PORT; port < TSD_END_PORT; port++) {
					String r = TSD + ":" + port;
					if (HttpWorker.TSDMap.get(TSD) == null) {
						HttpWorker.TSDMap.put(TSD, new LinkedBlockingQueue<String>());
					}

					//with load balancer we actually have 10 tsdb instances behind each port
					for(int j = 0; j < TSD_QUERIES_PER_PORT; j++) {
						HttpWorker.TSDMap.get(TSD).put(r);
					}
					LOG.info("Registering {}", r);
				}
			}
		}

		LOG.info("JedisClient Status: " + JedisClient.get().config());

		final Server server = new Server();

		Connector connector = new SelectChannelConnector();
		connector.setPort(PORT);
		server.addConnector(connector);

		QueuedThreadPool qtp = new QueuedThreadPool();
		qtp.setName("jetty-qt-pool");
		server.setThreadPool(qtp);

		ServletHandler servletHandler = new ServletHandler();
		servletHandler.addServletWithMapping(SplicerServlet.class.getName(), "/api/query");
		servletHandler.addServletWithMapping(SplicerServlet.class.getName(), "/api/query/query");
		servletHandler.addServletWithMapping(SplicerServlet.class.getName(), "/api/query/qexp");
		servletHandler.addServletWithMapping(ConfigServlet.class.getName(), "/api/config");
		servletHandler.addServletWithMapping(SuggestServlet.class.getName(), "/api/suggest");

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
