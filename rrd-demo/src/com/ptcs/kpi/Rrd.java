package com.ptcs.kpi;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.ArrayList;
import java.util.StringTokenizer;

/**
 * Java wrapper for Tobi Oetiker's RRDTOOL.
 *
 * <a href="http://www.rrdtool.org">RRDTOOL</a> is an excelent implementation of
 * the so-called <b>round robin database</b> (RRD) concept. Here is a short comment
 * on RRDTOOL from the author:<p>
 *
 * <i>It is pretty easy to gather status information from all sorts of things,
 * ranging from the temperature in your office to the number of octets which have
 * passed through the FDDI interface of your router. But it is not so trivial to
 * store this data in a <b>efficient</b> and <b>systematic</b> manner.
 * This is where rrdtool kicks in. It lets you log and analyze the data you
 * gather from all kinds of data-sources. The data analysis part of rrdtool is
 * based on the ability to quickly generate graphical representations of the data
 * values collected over a definable time period.</i><p>
 *
 * This class is part of RRDJTool package freely available from
 * <a href="http://marvin.datagate.co.yu:8081/rrdjtool"
 * target="blank">http://marvin.datagate.co.yu:8081/rrdjtool</a>.<p>
 *
 * RRDTOOL is written in C and comes with language bindings for Tcl, Perl, PHP... but
 * not for Java. If you want to use Oetiker's code from Java, you are forced to
 * use his command line utilities through inefficient <i>System.exec()</i> calls.<p>
 *
 * <b>Rrd</b> class uses <b>Java Native Interface</b> (JNI) approach to provide very fast,
 * simple and efficient RRD operation from your Java code. At the moment, RRD JNI is tested
 * on Linux platform only.
 *
 * <b>Bad news</b>: Before you start using Rrd.class you have to recompile <b>Rrd.c</b> for
 * your platform. The file can be found in the /native directory of this source code distribution.
 * Makefile for Linux is provided.<p>
 *
 * <b>Rrd</b> class has several methods to support the most important functionality of RRDTOOL:
 *
 * <ul>
 * 		<li>{@link #create(String) <b>create</b>} RRD database</li>
 * 		<li>{@link #update(String) <b>update</b>} database</li>
 * 		<li>{@link #graph(String) <b>graph</b>} generation</li>
 *		<li>{@link #fetch(String) <b>fetch</b>} data from the database</li>
 *		<li>get {@link #last(String) <b>last</b>} database update time</li>
 * </ul><p>
 *
 * Some less important RRDTOOL operations are not supported, and probably never will be
 * (<i>dump></i>, <i>restore</i>, <i>tune</i>...). These operations are used so rarely
 * that it would be a waste of time to provide java support for them.<p>
 *
 * All public methods are synchronized: RRD commands get processed one by one.<p>
 *
 * <b><u>IMPORTANT:</u></b> If you want to use Rrd.class, two shared libraries
 * must be present in your sistem:<p>
 *
 * <ul>
 * 		<li><b>librrd.so</b> - to create this library (if not already present), recompile
 * 		your RRDTOOL with <i>--enable-shared</i> option</li>
 *
 * 		<li><b>libjrrd.so</b> - this is a JNI wrapper for <i>librrd.so</i>. The source code of
 * 		this library is writen in C and provided along with the Rrd.java class in a single
 * 		file (<b>Rrd.c</b>). To compile it properly look at the
 * 		/native directory of this source code distribution and follow the instructions.</li>
 * </ul><p>
 *
 * Be sure to set your <i>LD_LIBRARY_PATH</i> environment variable so that both
 * <i>librrd.so</i> and <i>libjrrd.so</i> can be found by JVM.
 * Alternatively, you could place both shared libraries in your standard
 * library directory.<p>
 *
 * Before you use Rrd class, try {@link rrd.RrdDemo#main(String[]) <b>RrdDemo</b>}.
 * If it runs without exceptions, you are ready to use the full power of RRDTool
 * from the comfortable environment of Java.<p>
 *
 * <img src="rrd.png"><p>
 * @author Sasa Markovic<br><a href="mailto:sasam@dnseurope.co.uk">sasam@dnseurope.co.uk</a><p>
 * @version 1.0.1 (Feb 17, 2003)<p>
 *
 */
public class Rrd {
	private static final String JRRD_LIBRARY_NAME = "libjrrd.so";

	private static final String SHELL_CMD_PATTERN = "([^\" ]*\"[^\"]*\")|([^ \"]+)";
	private static final Pattern pattern = Pattern.compile(SHELL_CMD_PATTERN);
	private String osName = null;

	// Singleton pattern
	private static Rrd ourInstance;
	/**
	 * Returns single Rrd class instance for further usage.
	 *
	 * <b>Rrd</b> class follows the singleton pattern. Only one Rrd instance
	 * exists during the lifetime of application. This instance processes all
	 * RRD commands through synchronized methods.
	 *
	 * @return Rrd class instance to be used for RRD commands execution.
	 */
	public synchronized static Rrd getInstance() {
		if (ourInstance == null) {
			ourInstance = new Rrd();
		}
		return ourInstance;
	}

	// implemented through native calls
	private native int createRrdDatabase(String[] tokens);
	private native int updateRrdDatabase(String[] tokens);
	private native int createRrdGraph(String[] tokens);
	private native long getRrdLast(String[] tokens);
	private native int fetchRrdDatabase(String[] tokens);

	// helper native calls
	private native String getRrdError();
	private native String getRrdOutput();
	private native String[] getDsNames();
	private native double[] getDsValues();
	private native long[] getTimestamps();

	private Rrd() {
		osName = System.getProperty("os.name").toLowerCase();
		if (null != osName && -1 < osName.indexOf("linux")) {
			System.load(Rrd.class.getResource("/").toString().split(":")[1] + JRRD_LIBRARY_NAME);
		}
	}

	private static String[] getRrdCmdTokens(String rrdCmd) {
		Matcher m = pattern.matcher(rrdCmd);
		ArrayList tokens = new ArrayList();
		while(m.find()) {
			String token = m.group();
			tokens.add(token.replaceAll("\"", "").trim());
        }
		return (String[]) tokens.toArray(new String[0]);
	}

	private static void basicCheck(String[] tokens, String expectedRrdCmd) throws RrdException {
		if(tokens.length == 0) {
			throw new RrdException("Invalid RRD command");
		}
		else if(!tokens[0].equals(expectedRrdCmd)) {
			throw new RrdException("Invalid RRD command, " + expectedRrdCmd +
				" expected, got " + tokens[0]);
		}
	}

	/**
	 * Executes RRDCREATE command.
	 * This method is synchronized because RRD commands must be handled one by one.<p>
	 *
	 * Example:<p>
	 * <pre>
	 * String cmd = "create temperatures.rrd --start 999999999 --step 300 " +
	 *     "DS:temp1:GAUGE:1800:U:U " +
	 *     "DS:temp2:GAUGE:1800:U:U " +
	 *     "RRA:AVERAGE:0.5:1:600 RRA:AVERAGE:0.5:6:700 " +
	 *     "RRA:AVERAGE:0.5:24:700 RRA:AVERAGE:0.5:288:700 ";
	 * Rrd.getInstance().create(cmd);
	 * </pre>
	 *
	 * @param rrdCmd RRD command to execute. This command should follow <u>exactly</u> the same
	 * formatting rules as Oetiker's <i>rrdcreate</i> command line utility
	 * (see <i>man rrdcreate</i>). The command must start with <b>create</b>.
	 * @throws RrdException Exception thrown if create command fails.
	 */
	public synchronized void create(String rrdCmd) throws RrdException {
		String[] tokens = getRrdCmdTokens(rrdCmd);
		basicCheck(tokens, "create");
		int status = createRrdDatabase(tokens);
		if(status != 0) {
			throw new RrdException("RRDCREATE failed: " + getRrdError());
		}
	}

	/**
	 * Executes RRDUPDATE command.
	 * This method is synchronized because RRD commands must be handled one by one.<p>
	 *
	 * Example:<p>
	 * <pre>
	 * String cmd = "update temperatures.rrd 1004243132:32:46";
	 * Rrd.getInstance().update(cmd);
	 * </pre>
	 *
	 * @param rrdCmd RRD command to execute. This command should follow <u>exactly</u> the same
	 * formatting rules as Oetiker's <i>rrdupdate</i> command line utility
	 * (see <i>man rrdupdate</i>). The command must start with <b>update</b>.
	 * @throws RrdException Exception thrown if update command fails.
	 */
	public synchronized void update(String rrdCmd) throws RrdException {
		String[] tokens = getRrdCmdTokens(rrdCmd);
		basicCheck(tokens, "update");
		int status = updateRrdDatabase(tokens);
		if(status != 0) {
			throw new RrdException("RRDUPDATE failed: " + getRrdError());
		}
	}

    /**
	 * Executes RRDGRAPH command.
	 * This method is synchronized because RRD commands must be handled one by one.<p>
	 *
	 * Example:<p>
	 * <pre>
	 * String cmd = "graph temperatures.png -s 1000000000 -e 1000086400 -a PNG " +
	 *     "-w 450 -h 250 -t \"Moon temperatures\" " +
	 *     "-v \"Measured temperature\" " +
	 *     "DEF:temp1=temperatures.rrd:temp1:AVERAGE " +
	 *     "DEF:temp2=temperatures.rrd:temp2:AVERAGE " +
     *     "AREA:temp1#FF0000:shade " +
	 *     "GPRINT:temp1:AVERAGE:\"average %.2lf\\l\" " +
	 *     "LINE1:temp2#0000FF:sunshine " +
	 *     "GPRINT:temp2:AVERAGE:\"average %.2lf\\l\" " +
	 *     "PRINT:temp1:AVERAGE:%.2lf " +
	 *     "PRINT:temp2:AVERAGE:%.2lf ";
	 * String[] printInfo = Rrd.getInstance().graph(cmd);
	 * </pre>
	 *
	 * @param rrdCmd RRD command to execute. This command should follow <u>exactly</u> the same
	 * formatting rules as Oetiker's <i>rrdgraph</i> command line utility
	 * (see <i>man rrdgraph</i>). The command must start with <b>graph</b>.
	 * @return String array containing lines of text normaly printed on stdout. RRDGRAPH is used
	 * not only to create graphs but to perform various calculations. Those calculations are usually
	 * printed on the graph itself (GPRINT directives, see example above), or to standard output
	 * (PRINT directives). graph() method intercepts information that should be printed to stdout and
	 * returns it as array of Strings (one string represents one PRINT directive).
	 * @throws RrdException Exception thrown if graph command fails.
	 */
	public synchronized String[] graph(String rrdCmd) throws RrdException {
		String[] tokens = getRrdCmdTokens(rrdCmd);
		basicCheck(tokens, "graph");
		int status = createRrdGraph(tokens);
		if(status != 0) {
			throw new RrdException("RRDGRAPH failed: " + getRrdError());
		}
		String output = getRrdOutput();
		ArrayList outputLines = new ArrayList();
		StringTokenizer st = new StringTokenizer(output, "\n");
		while(st.hasMoreTokens()) {
			outputLines.add(st.nextToken());
		}
		return (String[]) outputLines.toArray(new String[0]);
	}

	/**
	 * Executes RRDLAST command.
	 * This method is synchronized because RRD commands must be handled one by one.<p>
	 *
	 * Example:<p>
	 * <pre>
	 * String cmd = "last temperatures.rrd";
	 * long timestamp = Rrd.getInstance().last(cmd);
	 * </pre>
	 *
	 * @param rrdCmd RRD command to execute. This command should follow <u>exactly</u> the same
	 * formatting rules as Oetiker's <i>rrdlast</i> command line utility
	 * (see <i>man rrdlast</i>). The command must start with <b>last</b>.
	 * @return UNIX timestamp of the last successful {@link #update(String) <b>update()</b>} call.
	 * @throws RrdException Exception thrown if command fails.
	 */
	public synchronized long last(String rrdCmd) throws RrdException {
		String[] tokens = getRrdCmdTokens(rrdCmd);
		basicCheck(tokens, "last");
		long last = getRrdLast(tokens);
		if(last == -1) {
			throw new RrdException("RRDLAST failed: " + getRrdError());
		}
		return last;
	}

	/**
	 * Executes RRDFETCH command.
	 * This method is synchronized because RRD commands must be handled one by one.<p>
	 *
	 * Example:<p>
	 * <pre>
	 * String cmd = "fetch temperatures.rrd AVERAGE --start 1000000000 --end 1000086400";
	 * rrd.Rrd.FetchData data = Rrd.getInstance().fetch(cmd);
	 * </pre>
	 *
	 * @param rrdCmd RRD command to execute. This command should follow <u>exactly</u> the same
	 * formatting rules as Oetiker's <i>rrdfetch</i> command line utility
	 * (see <i>man rrdfetch</i>). The command must start with <b>fetch</b>.
	 * @return Read only object of class {@link FetchData Rrd.FetchData} representing data fetched
	 * from the database.
	 * @throws RrdException Exception thrown if command fails.
	 */
	public synchronized FetchData fetch(String rrdCmd) throws RrdException {
		String[] tokens = getRrdCmdTokens(rrdCmd);
		basicCheck(tokens, "fetch");
		String[] dsNames = null;
		long[] times = null;
		double[] values = null;
		if (null != osName && -1 < osName.indexOf("linux")) {
			int status = fetchRrdDatabase(tokens);
			if(status == -1) {
				throw new RrdException("RRDFETCH failed: " + getRrdError());
			}
			dsNames = getDsNames();
			times = getTimestamps();
			values = getDsValues();
		}
		return new FetchData(dsNames, times, values);
	}

	/**
	 * Inner class to hold information returned by {@link Rrd#fetch(String) <b>Rrd.fetch()</b>}.
	 * Logically, this information represents one simple table. Here is the example:<p>
	 *
	 * <pre>
	 * timestamp     temperature1    temperature2
	 * ==========================================
	 * 1000000000        36.2           66.9
	 * 1000005000        39.3           69.2
	 * 1000010000        40.0           68.3
	 * ..........        ....           ....
	 * 1001000000        39.6           66.7
	 * ==========================================
	 * </pre>
	 *
	 * To read this data use the following methods:<p>
	 * <ul>
	 *     <li>{@link #getRowCount() <b>getRowCount()</b>} to find the number of rows
	 *     <li>{@link #getColCount() <b>getColCount()</b>} to find the number of <u>data</u> columns
	 *     <li>{@link #getColName(int) <b>getColName()</b>} to find the name of the i-th <u>data</u> column (zero based)
	 *     <li>{@link #getTimestamp(int) <b>getTimestamp(i)</b>} to find the timestamp of the i-th row (zero based)
	 *     <li>{@link #getValue(int,int) <b>getValue(i, j)</b>} to find the value of the j-th <u>data</u> column on the i-th row
	 * </ul> <p>
	 *
	 * Example:<p>
	 * <pre>
	 * String cmd = "fetch temperatures.rrd AVERAGE --start 1000000000 --end 1000086400";
	 * rrd.Rrd.FetchData data = Rrd.getInstance().fetch(cmd);
	 * int rows = data.getRowCount(), cols = data.getColCount();
	 * for(int i = 0; i < rows; i++) {
	 *     System.out.print(i + ": t=" + data.getTimestamp(i) + " ");
	 *     for(int j = 0; j < cols; j++) {
	 *         System.out.print(data.getColName(j) + "=" + data.getValue(i, j) + " ");
	 *     }
	 *     System.out.println();
	 * }
	 * </pre>
	 */
	public class FetchData {
		private String[] dsNames;
		private long tStart, tEnd, step;
		private double[] values;

		private int rowCount;
		private int colCount;

		private FetchData(String[] dsNames, long[] times, double[] values) {
			if (null != dsNames) {
				this.dsNames = dsNames;
				tStart = times[0]; tEnd = times[1]; step = times[2];
				this.values = values;
				rowCount = (int)((tEnd - tStart) / step + 1);
				colCount = dsNames.length;
			}
		}

		/**
		 * Returns timestamp of the given row.
		 * @param row Zero-based row number
		 * @return timestamp of the given row
		 */
		public long getTimestamp(int row) {
			return tStart + row * step;
		}

        /**
		 * Returns data source value from RRD database.
		 *
		 * @param row Zero-based row number.
		 * @param col Zero based data column number.
		 * @return value from the RRD database for the given row and column.
		 */
		public double getValue(int row, int col) {
			return values[row * colCount + col];
		}

		/**
		 * Returns column (data source) name for the given column number
		 * @param col Zero-based column number.
		 * @return Column (data source) name
		 */
		public String getColName(int col) {
			return dsNames[col];
		}

		/**
		 * Returns number of columns (data sources) generated by {@link Rrd#fetch(String) <b>Rrd.fetch()</b>} method.
		 * @return Number of columns
		 */
        public int getColCount() {
			return colCount;
		}

		/**
		 * Returns number of rows generated by {@link Rrd#fetch(String) <b>Rrd.fetch()</b>} method.
		 * @return Number of rows
		 */
		public int getRowCount() {
			return rowCount;
		}
	}
}
