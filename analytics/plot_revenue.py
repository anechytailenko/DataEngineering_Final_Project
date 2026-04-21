import matplotlib.pyplot as plt
import seaborn as sns
from analytics.config import get_db_connection, OUTPUT_DIR


def generate_plot():
    con = get_db_connection()
    print("Generating: Daily Revenue plot...")

    df_rev = con.execute(
        "SELECT shipment_date, total_revenue FROM mart_daily_revenue_summary ORDER BY shipment_date"
    ).df()

    plt.figure(figsize=(12, 5))
    sns.lineplot(
        data=df_rev, x="shipment_date", y="total_revenue", marker="o", color="b"
    )
    plt.title("Daily Total Revenue", fontsize=14)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/1_daily_revenue.png")
    plt.close()
