"""Interactive SQL shell over factory_analytics.chdb, for live demos (TV4).

    docker run --rm -it -v "${PWD}:/work" -v dairy-chdb:/chdb dairy-olap python -m olap_engineering.chdb_shell

End each statement with ';'. Type 'exit' to quit.
"""

try:
    import readline  # noqa: F401  arrow keys and history, if available
except ImportError:
    pass

from olap_engineering.chdb_analytics_engine import ChdbAnalyticsEngine


def main():
    engine = ChdbAnalyticsEngine()
    print("chDB shell - database 'dairy'. End statements with ';'. Type 'exit' to quit.")
    buf: list[str] = []
    while True:
        try:
            line = input("chdb> " if not buf else "   -> ")
        except EOFError:
            break
        if not buf and line.strip().lower() in {"exit", "quit"}:
            break
        buf.append(line)
        if not line.rstrip().endswith(";"):
            continue
        sql = "\n".join(buf).rstrip().rstrip(";")
        buf = []
        try:
            result = engine.sess.query(sql, "PrettyCompactMonoBlock")
            print(result, end="")
            print(f"({result.elapsed():.4f} s)\n")
        except Exception as exc:
            print(f"[error] {exc}\n")


if __name__ == "__main__":
    main()
