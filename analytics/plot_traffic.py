import matplotlib.pyplot as plt
import seaborn as sns
from analytics.config import get_db_connection, OUTPUT_DIR


def generate_plot():
    con = get_db_connection()
    print("Generating: Facility Traffic plot...")

    df_traffic = con.execute(
        "SELECT facility_name, total_scans FROM mart_facility_traffic ORDER BY total_scans DESC LIMIT 10"
    ).df()

    plt.figure(figsize=(12, 6))
    sns.barplot(
        data=df_traffic,
        x="total_scans",
        y="facility_name",
        palette="magma",
        hue="facility_name",
        legend=False,
    )
    plt.title("Top 10 Facilities by Traffic (Total Scans)", fontsize=14)
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/3_facility_traffic.png")
    plt.close()
