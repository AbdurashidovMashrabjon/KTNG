import os
from analyzer import ExcelAnalyzer

if __name__ == "__main__":
    # Ask user for Excel file
    file_path = input("📂 Please enter the path to your Excel file: ").strip()

    # Remove quotes if pasted with them
    file_path = file_path.strip('"').strip("'")

    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        exit(1)

    analyzer = ExcelAnalyzer(file_path)

    # Step 1: Load data
    df = analyzer.load_data()
    print("✅ Data loaded:")
    print(df.head())

    # Step 2: Basic stats
    print("\n📊 Basic analysis:")
    print(analyzer.basic_analysis())

    # Step 3: GPT insights
    print("\n🤖 GPT Insights:")
    print(analyzer.ask_chatgpt(df))

    # Step 4: Plot
    try:
        group_col = input("\n📌 Enter column name to group by (e.g. Region): ").strip()
        value_col = input("📌 Enter column name for values (e.g. Sales): ").strip()
        chart_path = analyzer.make_plot(group_col, value_col)
        print(f"\n📈 Chart saved: {chart_path}")
    except Exception as e:
        print(f"⚠️ Could not generate chart: {e}")
