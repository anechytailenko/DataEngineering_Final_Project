from analytics.config import setup_environment
import analytics.plot_revenue as plot_revenue
import analytics.plot_routes as plot_routes
import analytics.plot_traffic as plot_traffic
import analytics.plot_exceptions as plot_exceptions
import analytics.plot_weight as plot_weight


def run_all():
    print("--- Starting Analytics Visualizations ---")
    setup_environment()

    plot_revenue.generate_plot()
    plot_routes.generate_plot()
    plot_traffic.generate_plot()
    plot_exceptions.generate_plot()
    plot_weight.generate_plot()

    print("--- Success! All plots generated in /opt/airflow/analytics/plots ---")


if __name__ == "__main__":
    run_all()
