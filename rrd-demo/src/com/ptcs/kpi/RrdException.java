package com.ptcs.kpi;

/**
 * Exception thrown when {@link Rrd#create(String) Rrd.create()},
 * {@link Rrd#update(String) Rrd.update()}, {@link Rrd#graph(String) Rrd.graph()},
 * {@link Rrd#last(String) Rrd.last()} or {@link Rrd#fetch(String) Rrd.fetch()} fails.
 *
 * This class is part of RRDJTool package freely available from
 * <a href="http://marvin.datagate.co.yu:8081/rrdjtool"
 * target="blank">http://marvin.datagate.co.yu:8081/rrdjtool</a>.<p>
 *
 * @author Sasa Markovic<br><a href="mailto:sasam@dnseurope.co.uk">sasam@dnseurope.co.uk</a><p>
 * @version 1.0.1 (Feb 17, 2003)
 */
public class RrdException extends Exception {
	/**
	 * Constructor.
	 * @param message Human readable exception description.
	 */
	public RrdException(String message) {
		super(message);
	}
}
