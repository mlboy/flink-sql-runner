package run;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Lightweight Flink app to run SQL in FlinkSessionJob. Supports:
 * <ul>
 *   <li>{@code --sql "stmt1; stmt2"} — inline SQL (recommended for platform/CRD)</li>
 *   <li>{@code --sql-file /path/to/file.sql} — read SQL from file</li>
 * </ul>
 * Splits by ';', executes each statement; for DML (e.g. INSERT) blocks until job ends.
 *
 * <p>Usage:
 * <pre>
 *   ... flink-sql-runner.jar --sql "CREATE TABLE t (...); INSERT INTO t SELECT ..."
 *   ... flink-sql-runner.jar --sql-file /opt/flink/usrlib/sql/job.sql
 * </pre>
 */
public final class FlinkSqlRunner {

    private static final String OPT_SQL = "--sql";
    private static final String OPT_SQL_FILE = "--sql-file";
    private static final String STATEMENT_DELIMITER = ";";
    private static final String LINE_DELIMITER = "\n";
    private static final String COMMENT_PATTERN = "(--.*)|(((\\/\\*)+?[\\w\\W]+?(\\*\\/)+))";
    private static final Pattern SET_STATEMENT_PATTERN =
            Pattern.compile("SET\\s+'(\\S+)'\\s+=\\s+'(.*)'\\s*;?", Pattern.CASE_INSENSITIVE);
    private static final String BEGIN_CERTIFICATE = "-----BEGIN CERTIFICATE-----";
    private static final String END_CERTIFICATE = "-----END CERTIFICATE-----";
    private static final String ESCAPED_BEGIN_CERTIFICATE = "======BEGIN CERTIFICATE=====";
    private static final String ESCAPED_END_CERTIFICATE = "=====END CERTIFICATE=====";

    public static void main(String[] args) throws Exception {
        String sql = parseArgs(args);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        List<String> statements = splitStatements(sql);

        final JobClient[] lastJob = { null };
        for (String stmt : statements) {
            String s = stmt.trim();
            if (s.isEmpty()) {
                continue;
            }

            if (applySetStatement(tableEnv, s)) {
                continue;
            }

            TableResult result = tableEnv.executeSql(s);
            result.getJobClient().ifPresent(jc -> lastJob[0] = jc);
        }

        if (lastJob[0] != null) {
            try {
                lastJob[0].getJobExecutionResult().get();
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                // Web Submission / detached 模式下不支持 getJobExecutionResult，任务已提交成功，直接退出
                if (cause != null && isWebSubmissionUnsupported(cause)) {
                    return;
                }
                throw new RuntimeException(e);
            }
        }
    }

    /** Web Submission 时 JobClient 不支持获取执行结果，通过异常信息判断。 */
    private static boolean isWebSubmissionUnsupported(Throwable t) {
        String msg = t.getMessage();
        if (msg != null && msg.contains("Web Submission") && msg.contains("cannot be fetched")) {
            return true;
        }
        return t.getCause() != null && isWebSubmissionUnsupported(t.getCause());
    }

    /**
     * Parse CLI: --sql "..." or --sql-file /path. Returns the SQL string.
     */
    private static String parseArgs(String[] args) throws Exception {
        if (args.length < 2) {
            printUsage();
            System.exit(1);
        }
        String opt = args[0];
        if (OPT_SQL.equals(opt)) {
            return args[1];
        }
        if (OPT_SQL_FILE.equals(opt)) {
            String path = args[1];
            return new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8);
        }
        printUsage();
        System.exit(1);
        return null;
    }

    private static void printUsage() {
        System.err.println("Usage:");
        System.err.println("  --sql \"SQL statements (semicolon-separated)\"");
        System.err.println("  --sql-file /path/to/file.sql");
    }

    private static List<String> splitStatements(String sql) {
        String formatted =
                formatSqlFile(sql)
                        .replace(BEGIN_CERTIFICATE, ESCAPED_BEGIN_CERTIFICATE)
                        .replace(END_CERTIFICATE, ESCAPED_END_CERTIFICATE)
                        .replaceAll(COMMENT_PATTERN, "")
                        .replace(ESCAPED_BEGIN_CERTIFICATE, BEGIN_CERTIFICATE)
                        .replace(ESCAPED_END_CERTIFICATE, END_CERTIFICATE);

        List<String> statements = new ArrayList<>();
        StringBuilder current = null;
        boolean statementSet = false;

        for (String line : formatted.split(LINE_DELIMITER)) {
            String trimmed = line.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            if (current == null) {
                current = new StringBuilder();
            }

            if (startsWithIgnoreCase(trimmed, "EXECUTE STATEMENT SET")) {
                statementSet = true;
            }

            current.append(trimmed).append(LINE_DELIMITER);

            if (trimmed.endsWith(STATEMENT_DELIMITER)) {
                if (!statementSet || equalsIgnoreCase(trimmed, "END;")) {
                    statements.add(current.toString());
                    current = null;
                    statementSet = false;
                }
            }
        }

        return statements;
    }

    private static String formatSqlFile(String content) {
        String trimmed = content.trim();
        StringBuilder formatted = new StringBuilder(trimmed);
        if (!trimmed.endsWith(STATEMENT_DELIMITER)) {
            formatted.append(STATEMENT_DELIMITER);
        }
        formatted.append(LINE_DELIMITER);
        return formatted.toString();
    }

    private static boolean applySetStatement(TableEnvironment tableEnv, String statement) {
        Matcher matcher = SET_STATEMENT_PATTERN.matcher(statement);
        if (!matcher.matches()) {
            return false;
        }
        String key = matcher.group(1);
        String value = matcher.group(2);
        tableEnv.getConfig().getConfiguration().setString(key, value);
        return true;
    }

    private static boolean startsWithIgnoreCase(String text, String prefix) {
        return text.regionMatches(true, 0, prefix, 0, prefix.length());
    }

    private static boolean equalsIgnoreCase(String a, String b) {
        return a.equalsIgnoreCase(b);
    }
}
