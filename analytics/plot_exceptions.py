import matplotlib.pyplot as plt
import seaborn as sns
from analytics.config import get_db_connection, OUTPUT_DIR


def generate_plot():
    con = get_db_connection()
    print("Generating: Exceptions Breakdown plot...")

    df_exc = con.execute(
        "SELECT exception_type, count(event_id) as cnt FROM mart_exceptions_analysis GROUP BY exception_type"
    ).df()

    if not df_exc.empty:
        plt.figure(figsize=(8, 8))
        plt.pie(
            df_exc["cnt"],
            labels=df_exc["exception_type"],
            autopct="%1.1f%%",
            startangle=140,
            colors=sns.color_palette("pastel"),
        )
        plt.title("Exceptions Breakdown", fontsize=14)
        plt.savefig(f"{OUTPUT_DIR}/4_exceptions_breakdown.png")
        plt.close()
