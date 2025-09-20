from spark_application_analyzer import analyze_application
from spark_application_analyzer.models.recommendation import Recommendation

# --- Example 1: Using an EMR Cluster ID ---
try:
    print("--- Analyzing application using EMR ID ---")
    # The function will automatically discover the Spark History Server URL
    recommendation: Recommendation = analyze_application(
        application_id="application_1756176332935_0487",  # Replace with your app ID
    )

    print("\n--- Analysis Complete ---")
    print(f"Application: {recommendation.app_name}")
    print(f"Recommended Executor Heap: {recommendation.suggested_heap_in_gb} GB")
    print(
        f"Recommended Executor Overhead: {recommendation.suggested_overhead_in_gb} GB"
    )
    print(f"Recommended Max Executors: {recommendation.recommended_maxExecutors}")

except Exception as e:
    print(f"An error occurred: {e}")
