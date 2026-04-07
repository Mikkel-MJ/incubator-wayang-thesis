package org.apache.wayang.ml.benchmarks;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.TextFileSource;
import org.apache.wayang.basic.operators.TableSource;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.basic.types.RecordType;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;

import java.sql.Timestamp;
import java.util.function.BiFunction;
import java.util.List;
import java.util.Map;
import java.util.Comparator;
import java.util.HashSet;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.util.Set;

public class STATSSources {

    // Maps table names directly to their corresponding parse method
    public static final Map<String, BiFunction<String, String, Object[]>> TABLE_PARSERS = Map.of(
        "users",       STATSSources::parseUsers,
        "posts",       STATSSources::parsePosts,
        "postlinks",   STATSSources::parsePostLinks,
        "posthistory", STATSSources::parsePostHistory,
        "comments",    STATSSources::parseComments,
        "votes",       STATSSources::parseVotes,
        "badges",      STATSSources::parseBadges,
        "tags",        STATSSources::parseTags
    );

    public static final String DELIMITER = ",";


    // -------------------------------------------------------------------------
    // Null-safe parsing helpers
    // -------------------------------------------------------------------------

    private static Integer parseIntOrNull(String[] cols, int index) {
        if (index >= cols.length) return null;
        String value = cols[index];
        if (value == null || value.isBlank()) return null;
        return Integer.parseInt(value.trim());
    }

    private static Short parseShortOrNull(String[] cols, int index) {
        if (index >= cols.length) return null;
        String value = cols[index];
        if (value == null || value.isBlank()) return null;
        return Short.parseShort(value.trim());
    }

    private static Timestamp parseTimestampOrNull(String[] cols, int index) {
        if (index >= cols.length) return null;
        String value = cols[index];
        if (value == null || value.isBlank()) return null;
        return Timestamp.valueOf(value.trim());
    }

    // -------------------------------------------------------------------------
    // setSources
    // -------------------------------------------------------------------------

    /**
     * Replaces up to {@code nrOfReplacements} sources in the given Wayang plan
     * with CSV file sources read from {@code dataPath}, using the STATS benchmark schema.
     *
     * @param wayangPlan       the Wayang plan builder whose sources are to be replaced
     * @param dataPath         the directory path containing the STATS CSV files
     * @param nrOfReplacements the number of sources to replace in the plan
     */
    public static void setSources(WayangPlan plan, String dataPath, int nrOfReplacements) {
        final List<Operator> sources = plan.collectReachableTopLevelSources()
            .stream()
            .map(op -> (TableSource) op)
            .sorted(Comparator.comparing(op -> op.getTableName()))
            .collect(Collectors.toList());

        Set<String> replacedSources = new HashSet<>();
        int nrOfSourcesReplaced = 0;

        for (Operator op : sources) {
            if (nrOfSourcesReplaced >= nrOfReplacements) {
                return;
            }

            if (op instanceof TableSource) {
                String tableName = ((TableSource) op).getTableName();
                String filePath = dataPath + tableName + ".csv";
                if (!replacedSources.contains(tableName)) {
                    BiFunction<String, String, Object[]> parsingFunction = TABLE_PARSERS.get(tableName);
                    TextFileSource replacement = new TextFileSource(filePath, "UTF-8");

                    MapOperator<String, Record> parser = new MapOperator<>(
                        (line) -> {
                            return new Record(
                                parsingFunction.apply(
                                    line,
                                    DELIMITER
                                )
                            );
                        },
                        String.class,
                        Record.class
                    );
                    OutputSlot.stealConnections(op, parser);

                    replacement.connectTo(0, parser, 0);
                    replacedSources.add(tableName);
                    nrOfSourcesReplaced++;
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Parse methods
    // -------------------------------------------------------------------------

    /**
     * Parses a single CSV line into a users row.
     * Schema:
     * <ul>
     *   <li>[0] Id            (Integer)   – NOT NULL</li>
     *   <li>[1] Reputation    (Integer)   – NOT NULL</li>
     *   <li>[2] CreationDate  (Timestamp) – NOT NULL</li>
     *   <li>[3] Views         (Integer)   – NOT NULL</li>
     *   <li>[4] UpVotes       (Integer)   – NOT NULL</li>
     *   <li>[5] DownVotes     (Integer)   – NOT NULL</li>
     * </ul>
     *
     * @param line      a single CSV line
     * @param delimiter column delimiter (e.g. ",")
     * @return Object[] holding typed column values
     */
    public static Object[] parseUsers(String line, String delimiter) {
        String[] cols = line.split(delimiter);
        return new Object[]{
            parseIntOrNull(cols, 0),        // Id            NOT NULL
            parseIntOrNull(cols, 1),        // Reputation    NOT NULL
            parseTimestampOrNull(cols, 2),  // CreationDate  NOT NULL
            parseIntOrNull(cols, 3),        // Views         NOT NULL
            parseIntOrNull(cols, 4),        // UpVotes       NOT NULL
            parseIntOrNull(cols, 5)         // DownVotes     NOT NULL
        };
    }

    /**
     * Parses a single CSV line into a posts row.
     * Schema:
     * <ul>
     *   <li>[0] Id                (Integer)   – NOT NULL</li>
     *   <li>[1] PostTypeId        (Short)     – NOT NULL</li>
     *   <li>[2] CreationDate      (Timestamp) – NOT NULL</li>
     *   <li>[3] Score             (Integer)   – NOT NULL</li>
     *   <li>[4] ViewCount         (Integer)   – NULLABLE</li>
     *   <li>[5] OwnerUserId       (Integer)   – NULLABLE</li>
     *   <li>[6] AnswerCount       (Integer)   – NULLABLE</li>
     *   <li>[7] CommentCount      (Integer)   – NULLABLE</li>
     *   <li>[8] FavoriteCount     (Integer)   – NULLABLE</li>
     *   <li>[9] LastEditorUserId  (Integer)   – NULLABLE</li>
     * </ul>
     *
     * @param line      a single CSV line
     * @param delimiter column delimiter (e.g. ",")
     * @return Object[] holding typed column values
     */
    public static Object[] parsePosts(String line, String delimiter) {
        String[] cols = line.split(delimiter);
        return new Object[]{
            parseIntOrNull(cols, 0),        // Id                NOT NULL
            parseShortOrNull(cols, 1),      // PostTypeId        NOT NULL
            parseTimestampOrNull(cols, 2),  // CreationDate      NOT NULL
            parseIntOrNull(cols, 3),        // Score             NOT NULL
            parseIntOrNull(cols, 4),        // ViewCount         NULLABLE
            parseIntOrNull(cols, 5),        // OwnerUserId       NULLABLE
            parseIntOrNull(cols, 6),        // AnswerCount       NULLABLE
            parseIntOrNull(cols, 7),        // CommentCount      NULLABLE
            parseIntOrNull(cols, 8),        // FavoriteCount     NULLABLE
            parseIntOrNull(cols, 9)         // LastEditorUserId  NULLABLE
        };
    }

    /**
     * Parses a single CSV line into a postLinks row.
     * Schema:
     * <ul>
     *   <li>[0] Id             (Integer)   – NOT NULL</li>
     *   <li>[1] CreationDate   (Timestamp) – NOT NULL</li>
     *   <li>[2] PostId         (Integer)   – NOT NULL</li>
     *   <li>[3] RelatedPostId  (Integer)   – NOT NULL</li>
     *   <li>[4] LinkTypeId     (Short)     – NOT NULL</li>
     * </ul>
     *
     * @param line      a single CSV line
     * @param delimiter column delimiter (e.g. ",")
     * @return Object[] holding typed column values
     */
    public static Object[] parsePostLinks(String line, String delimiter) {
        String[] cols = line.split(delimiter);
        return new Object[]{
            parseIntOrNull(cols, 0),        // Id             NOT NULL
            parseTimestampOrNull(cols, 1),  // CreationDate   NOT NULL
            parseIntOrNull(cols, 2),        // PostId         NOT NULL
            parseIntOrNull(cols, 3),        // RelatedPostId  NOT NULL
            parseShortOrNull(cols, 4)       // LinkTypeId     NOT NULL
        };
    }

    /**
     * Parses a single CSV line into a postHistory row.
     * Schema:
     * <ul>
     *   <li>[0] Id                  (Integer)   – NOT NULL</li>
     *   <li>[1] PostHistoryTypeId   (Short)     – NOT NULL</li>
     *   <li>[2] PostId              (Integer)   – NOT NULL</li>
     *   <li>[3] CreationDate        (Timestamp) – NOT NULL</li>
     *   <li>[4] UserId              (Integer)   – NULLABLE</li>
     * </ul>
     *
     * @param line      a single CSV line
     * @param delimiter column delimiter (e.g. ",")
     * @return Object[] holding typed column values
     */
    public static Object[] parsePostHistory(String line, String delimiter) {
        String[] cols = line.split(delimiter);
        return new Object[]{
            parseIntOrNull(cols, 0),        // Id                NOT NULL
            parseShortOrNull(cols, 1),      // PostHistoryTypeId NOT NULL
            parseIntOrNull(cols, 2),        // PostId            NOT NULL
            parseTimestampOrNull(cols, 3),  // CreationDate      NOT NULL
            parseIntOrNull(cols, 4)         // UserId            NULLABLE
        };
    }

    /**
     * Parses a single CSV line into a comments row.
     * Schema:
     * <ul>
     *   <li>[0] Id            (Integer)   – NOT NULL</li>
     *   <li>[1] PostId        (Integer)   – NOT NULL</li>
     *   <li>[2] Score         (Short)     – NOT NULL</li>
     *   <li>[3] CreationDate  (Timestamp) – NOT NULL</li>
     *   <li>[4] UserId        (Integer)   – NULLABLE</li>
     * </ul>
     *
     * @param line      a single CSV line
     * @param delimiter column delimiter (e.g. ",")
     * @return Object[] holding typed column values
     */
    public static Object[] parseComments(String line, String delimiter) {
        String[] cols = line.split(delimiter);
        return new Object[]{
            parseIntOrNull(cols, 0),        // Id            NOT NULL
            parseIntOrNull(cols, 1),        // PostId        NOT NULL
            parseShortOrNull(cols, 2),      // Score         NOT NULL
            parseTimestampOrNull(cols, 3),  // CreationDate  NOT NULL
            parseIntOrNull(cols, 4)         // UserId        NULLABLE
        };
    }

    /**
     * Parses a single CSV line into a votes row.
     * Schema:
     * <ul>
     *   <li>[0] Id            (Integer)   – NOT NULL</li>
     *   <li>[1] PostId        (Integer)   – NOT NULL</li>
     *   <li>[2] VoteTypeId    (Short)     – NOT NULL</li>
     *   <li>[3] CreationDate  (Timestamp) – NOT NULL</li>
     *   <li>[4] UserId        (Integer)   – NULLABLE</li>
     *   <li>[5] BountyAmount  (Short)     – NULLABLE</li>
     * </ul>
     *
     * @param line      a single CSV line
     * @param delimiter column delimiter (e.g. ",")
     * @return Object[] holding typed column values
     */
    public static Object[] parseVotes(String line, String delimiter) {
        String[] cols = line.split(delimiter);
        return new Object[]{
            parseIntOrNull(cols, 0),        // Id            NOT NULL
            parseIntOrNull(cols, 1),        // PostId        NOT NULL
            parseShortOrNull(cols, 2),      // VoteTypeId    NOT NULL
            parseTimestampOrNull(cols, 3),  // CreationDate  NOT NULL
            parseIntOrNull(cols, 4),        // UserId        NULLABLE
            parseShortOrNull(cols, 5)       // BountyAmount  NULLABLE
        };
    }

    /**
     * Parses a single CSV line into a badges row.
     * Schema:
     * <ul>
     *   <li>[0] Id      (Integer)   – NOT NULL</li>
     *   <li>[1] UserId  (Integer)   – NOT NULL</li>
     *   <li>[2] Date    (Timestamp) – NOT NULL</li>
     * </ul>
     *
     * @param line      a single CSV line
     * @param delimiter column delimiter (e.g. ",")
     * @return Object[] holding typed column values
     */
    public static Object[] parseBadges(String line, String delimiter) {
        String[] cols = line.split(delimiter);
        return new Object[]{
            parseIntOrNull(cols, 0),        // Id      NOT NULL
            parseIntOrNull(cols, 1),        // UserId  NOT NULL
            parseTimestampOrNull(cols, 2)   // Date    NOT NULL
        };
    }

    /**
     * Parses a single CSV line into a tags row.
     * Schema:
     * <ul>
     *   <li>[0] Id             (Integer) – NOT NULL</li>
     *   <li>[1] Count          (Integer) – NOT NULL</li>
     *   <li>[2] ExcerptPostId  (Integer) – NULLABLE</li>
     * </ul>
     *
     * @param line      a single CSV line
     * @param delimiter column delimiter (e.g. ",")
     * @return Object[] holding typed column values
     */
    public static Object[] parseTags(String line, String delimiter) {
        String[] cols = line.split(delimiter);
        return new Object[]{
            parseIntOrNull(cols, 0),        // Id             NOT NULL
            parseIntOrNull(cols, 1),        // Count          NOT NULL
            parseIntOrNull(cols, 2)         // ExcerptPostId  NULLABLE
        };
    }
}
