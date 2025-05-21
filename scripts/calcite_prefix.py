import re

# Set of SQL reserved keywords to avoid as aliases
RESERVED_KEYWORDS = {word.lower(): f"{word.lower()}_alias" for word in [
    "abort", "abs", "absolute", "access", "action", "add", "admin", "after", "aggregate", "alias", "all", "allocate",
    "alter", "analyse", "analyze", "and", "any", "are", "array", "as", "asc", "asensitive", "assertion", "associate",
    "asymmetric", "at", "atomic", "authorization", "avg", "backup", "before", "begin", "between", "bigint", "binary",
    "bit", "blob", "boolean", "both", "breadth", "by", "c", "cache", "call", "called", "cardinality", "cascade",
    "cascaded", "case", "cast", "catalog", "ceil", "ceiling", "chain", "char", "character", "check",
    "cluster", "column", "commit", "constraint", "convert", "copy", "count", "create", "cross",
    "current", "cursor", "cycle", "data", "database", "date", "day", "dec", "decimal", "declare",
    "default", "delete", "dense_rank", "desc", "describe", "distinct", "do", "drop", "dynamic",
    "each", "else", "enable", "end", "equals", "except", "execute", "exists", "exp", "explain",
    "false", "fetch", "filter", "float", "flush", "following", "for", "foreign", "format",
    "from", "full", "function", "grant", "group", "having", "hour", "identity", "if",
    "ignore", "ilike", "immediate", "in", "index", "inner", "insert", "int", "integer",
    "intersect", "interval", "into", "is", "isolation", "join", "json", "key", "kill",
    "lag", "language", "large", "last", "lead", "left", "level", "like", "limit",
    "load", "local", "localtime", "localtimestamp", "lock", "login", "long", "loop",
    "lower", "map", "match", "max", "merge", "min", "minute", "mod", "mode",
    "modify", "module", "month", "move", "name", "national", "natural", "nchar",
    "new", "next", "no", "not", "null", "nullif", "number", "numeric", "object",
    "offset", "old", "on", "only", "open", "operation", "option", "or", "order",
    "outer", "output", "over", "overlaps", "overlay", "owner", "parameter", "partition",
    "percent", "pivot", "position", "power", "precision", "prepare", "primary",
    "procedure", "public", "raise", "range", "rank", "read", "real", "recursive",
    "ref", "references", "referencing", "refresh", "regr_avgx", "regr_avgy", "regr_count",
    "regr_intercept", "regr_r2", "regr_slope", "regr_sxx", "regr_sxy", "regr_syy", "relative",
    "release", "rename", "repeat", "replace", "reset", "restrict", "return", "revoke",
    "right", "role", "rollback", "row", "row_number", "rows", "rule", "savepoint",
    "schema", "scroll", "search", "second", "section", "select", "sensitive", "sequence",
    "session", "session_user", "set", "sets", "show", "similar", "size", "some",
    "specific", "sql", "sqlcode", "sqlerror", "sqlstate", "start", "state", "statistics",
    "substring", "sum", "symmetric", "system", "system_user", "table", "tablesample",
    "temporary", "then", "time", "timestamp", "timezone_hour", "timezone_minute", "to",
    "trailing", "transaction", "translate", "translation", "treat", "trigger", "true",
    "truncate", "unbounded", "union", "unique", "unknown", "update", "upper", "usage",
    "user", "using", "value", "values", "varchar", "varying", "view", "when", "where",
    "window", "with", "within", "without", "work", "write", "xml", "year", "zone"
]}

def rename_reserved_aliases(query):
    """Finds and renames reserved SQL keywords used as aliases in the query."""
    alias_pattern = r'\bAS\s+(\w+)\b'

    def replace_alias(match):
        alias = match.group(1)
        if alias.lower() in RESERVED_KEYWORDS:
            return f"AS {RESERVED_KEYWORDS[alias.lower()]}"
        return match.group(0)  # Return unchanged if not a reserved word

    return re.sub(alias_pattern, replace_alias, query, flags=re.IGNORECASE)

def replace_not_equal_operator(query):
    """Replaces every '!=' operator with '<>' to standardize SQL syntax."""
    return query.replace("!=", "<>")

def add_postgres_prefix_to_tables(query):
    """Prefixes 'postgres.' to all table names in the FROM clause, trims whitespace, and renames aliases."""
    from_clause_regex = r'FROM\s+([\w, \(\)\s]+)'

    match = re.search(from_clause_regex, query, re.IGNORECASE)

    if match:
        from_clause = match.group(1)
        tables = re.split(r'\s*,\s*', from_clause)  # Split tables by commas
        prefixed_tables = [f"postgres.{table.strip()}" for table in tables]
        new_from_clause = 'FROM ' + ', '.join(prefixed_tables)
        query = query.replace(match.group(0), new_from_clause)

    # Rename reserved keywords used as aliases
    query = rename_reserved_aliases(query)

    # Replace '!=' with '<>'
    query = replace_not_equal_operator(query)

    # Trim excess whitespace
    query = re.sub(r'\s+', ' ', query).strip()

    return query

def process_queries(file_path, output_dir):
    """Reads queries from a file, modifies them, and saves each to its own file."""
    with open(file_path, 'r') as file:
        queries = file.readlines()

    for query in queries:
        match = re.match(r'q(\d+)#####(SELECT.*)', query, re.IGNORECASE)
        if match:
            query_number = match.group(1)
            sql_query = match.group(2).strip()

            modified_query = add_postgres_prefix_to_tables(sql_query)

            output_file = f"{output_dir}/g_{query_number}.sql"
            with open(output_file, 'w') as f:
                f.write(modified_query)

            print(f"Processed and saved: {output_file}")

# Example usage
input_file = "/var/www/html/wayang-plugins/wayang-ml/src/main/resources/calcite-ready-job-queries/training/job.txt"  # Change this to your actual file
output_directory = "/var/www/html/wayang-plugins/wayang-ml/src/main/resources/calcite-ready-job-queries/training"  # Directory where queries will be saved

import os
os.makedirs(output_directory, exist_ok=True)  # Ensure output directory exists

process_queries(input_file, output_directory)

