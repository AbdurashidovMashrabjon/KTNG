import os
import pandas as pd
import matplotlib.pyplot as plt
from openai import OpenAI
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

class ExcelAnalyzer:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.df = None

        # Load API key from environment
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("❌ OPENAI_API_KEY is not set. Add it to .env file.")

        self.client = OpenAI(api_key=api_key)

    def load_data(self):
        """Load Excel file into pandas DataFrame"""
        self.df = pd.read_excel(self.file_path)
        return self.df

    def basic_analysis(self):
        """Return descriptive statistics of the dataset"""
        if self.df is None:
            raise ValueError("No data loaded. Call load_data() first.")
        return self.df.describe(include="all")

    def ask_chatgpt(self, df: pd.DataFrame):
        """Send dataframe summary to ChatGPT for analysis"""
        summary = df.describe().to_string()

        prompt = f"""
        You are a data analyst. 
        Analyze this dataset summary and provide insights, trends, or problems a business manager should know:

        {summary}
        """

        response = self.client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}]
        )

        return response.choices[0].message.content

    def make_plot(self, group_col: str, value_col: str):
        """Generate bar chart grouped by one column"""
        if self.df is None:
            raise ValueError("No data loaded. Call load_data() first.")

        chart = self.df.groupby(group_col)[value_col].sum()

        plt.figure(figsize=(6,4))
        chart.plot(kind="bar")
        plt.title(f"{value_col} by {group_col}")
        plt.xlabel(group_col)
        plt.ylabel(value_col)
        plt.tight_layout()

        os.makedirs("dashboards", exist_ok=True)
        save_path = f"dashboards/{group_col}_{value_col}.png"
        plt.savefig(save_path)
        plt.close()
        return save_path
