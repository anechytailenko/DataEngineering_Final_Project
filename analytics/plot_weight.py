import matplotlib.pyplot as plt
import seaborn as sns
from analytics.config import get_db_connection, OUTPUT_DIR


def generate_plot():
    con = get_db_connection()
    print("Generating: Weight Pricing plot...")

    df_weight = con.execute(
        "SELECT weight_bracket, avg_cost_per_shipment FROM mart_weight_brackets_pricing"
    ).df()

    plt.figure(figsize=(8, 5))
    sns.barplot(
        data=df_weight,
        x="weight_bracket",
        y="avg_cost_per_shipment",
        palette="coolwarm",
        hue="weight_bracket",
        legend=False,
    )
    plt.title("Average Shipping Cost by Weight Bracket", fontsize=14)
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/5_weight_pricing.png")
    plt.close()
