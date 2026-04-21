import matplotlib.pyplot as plt
import seaborn as sns
from analytics.config import get_db_connection, OUTPUT_DIR


def generate_plot():
    con = get_db_connection()
    print("Generating: Top Routes plot...")

    df_route = con.execute(
        """
        SELECT origin_city || ' -> ' || destination_city as route, total_shipments 
        FROM mart_route_performance 
        ORDER BY total_shipments DESC LIMIT 10
    """
    ).df()

    plt.figure(figsize=(10, 6))
    sns.barplot(
        data=df_route,
        x="total_shipments",
        y="route",
        palette="viridis",
        hue="route",
        legend=False,
    )
    plt.title("Top 10 Busiest Routes", fontsize=14)
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/2_top_routes.png")
    plt.close()
