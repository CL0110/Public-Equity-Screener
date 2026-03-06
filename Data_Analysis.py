"""
Comprehensive Company Data Analysis Program
Features: Data loading, cleaning, visualization, statistical analysis, and reporting
Compatible with Excel files containing Company Data, Company Officers, and Corporate Actions
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import warnings
warnings.filterwarnings('ignore')

from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CompanyDataAnalyzer:
    """
    Comprehensive data analyzer for company financial and operational data
    """
    
    def __init__(self, excel_file_path: str):
        """Initialize the analyzer with Excel file path"""
        self.excel_file_path = excel_file_path
        self.company_data = None
        self.officers_data = None
        self.corporate_actions = None
        self.analysis_results = {}
        
        # Set up plotting style
        plt.style.use('seaborn-v0_8-darkgrid')
        sns.set_palette("husl")
        
    def load_data(self) -> bool:
        """Load data from Excel file"""
        try:
            logger.info(f"Loading data from {self.excel_file_path}")
            
            # Load all sheets
            excel_data = pd.ExcelFile(self.excel_file_path)
            
            # Load Company Data (Sheet 1)
            self.company_data = pd.read_excel(self.excel_file_path, sheet_name=0)
            logger.info(f"Loaded {len(self.company_data)} company records")
            
            # Load Company Officers (Sheet 2)
            if len(excel_data.sheet_names) > 1:
                self.officers_data = pd.read_excel(self.excel_file_path, sheet_name=1)
                logger.info(f"Loaded {len(self.officers_data)} officer records")
            
            # Load Corporate Actions (Sheet 3)
            if len(excel_data.sheet_names) > 2:
                self.corporate_actions = pd.read_excel(self.excel_file_path, sheet_name=2)
                logger.info(f"Loaded {len(self.corporate_actions)} corporate actions")
                
            return True
            
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            return False
    
    def clean_and_prepare_data(self):
        """Clean and prepare data for analysis"""
        logger.info("Cleaning and preparing data...")
        
        if self.company_data is not None:
            # Clean company data
            self.company_data = self._clean_company_data(self.company_data)
            
        if self.officers_data is not None:
            # Clean officers data
            self.officers_data = self._clean_officers_data(self.officers_data)
            
        if self.corporate_actions is not None:
            # Clean corporate actions data
            self.corporate_actions = self._clean_corporate_actions(self.corporate_actions)
            
    def _clean_company_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean company data"""
        df = df.copy()
        
        # Convert numeric columns
        numeric_columns = [
            'Previous Closing Price', 'Market Capitalization', 'Trading Volume',
            'Trailing Price-to-Earnings Ratio', 'Total Revenue', 'Net Income to Common Shareholders',
            'Total Cash', 'Total Debt', 'Full-Time Employees'
        ]
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Convert date columns from timestamp
        date_columns = [
            'Ex-Dividend Date Timestamp', 'Last Fiscal Year End Timestamp',
            'Next Fiscal Year End Timestamp'
        ]
        
        for col in date_columns:
            if col in df.columns:
                df[f"{col.replace(' Timestamp', '')}_Date"] = pd.to_datetime(
                    df[col], unit='s', errors='coerce'
                )
        
        return df
    
    def _clean_officers_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean officers data"""
        df = df.copy()
        
        # Convert numeric columns
        numeric_columns = ['Officer Age', 'Officer Year of Birth', 'Total Compensation']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    
    def _clean_corporate_actions(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean corporate actions data"""
        df = df.copy()
        
        # Convert event date from epoch milliseconds
        if 'Event Date (Epoch Milliseconds)' in df.columns:
            df['Event_Date'] = pd.to_datetime(
                df['Event Date (Epoch Milliseconds)'], unit='ms', errors='coerce'
            )
        
        # Convert amount to numeric
        if 'Amount' in df.columns:
            df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')
            
        return df
    
    def generate_data_overview(self) -> Dict:
        """Generate comprehensive data overview"""
        logger.info("Generating data overview...")
        
        overview = {
            'company_data': {},
            'officers_data': {},
            'corporate_actions': {}
        }
        
        # Company data overview
        if self.company_data is not None:
            overview['company_data'] = {
                'total_companies': len(self.company_data),
                'unique_sectors': self.company_data['Sector Display Name'].nunique() if 'Sector Display Name' in self.company_data.columns else 0,
                'unique_industries': self.company_data['Industry Display Name'].nunique() if 'Industry Display Name' in self.company_data.columns else 0,
                'data_completeness': (self.company_data.notna().sum() / len(self.company_data) * 100).round(2),
                'market_cap_stats': self._get_market_cap_stats()
            }
        
        # Officers data overview
        if self.officers_data is not None:
            overview['officers_data'] = {
                'total_officers': len(self.officers_data),
                'unique_companies': self.officers_data['Ticker'].nunique() if 'Ticker' in self.officers_data.columns else 0,
                'avg_age': self.officers_data['Officer Age'].mean() if 'Officer Age' in self.officers_data.columns else None,
                'compensation_stats': self._get_compensation_stats()
            }
        
        # Corporate actions overview
        if self.corporate_actions is not None:
            overview['corporate_actions'] = {
                'total_actions': len(self.corporate_actions),
                'action_types': self.corporate_actions['Action Type'].value_counts().to_dict() if 'Action Type' in self.corporate_actions.columns else {},
                'date_range': self._get_actions_date_range()
            }
        
        self.analysis_results['overview'] = overview
        return overview
    
    def _get_market_cap_stats(self) -> Dict:
        """Get market capitalization statistics"""
        if 'Market Capitalization' not in self.company_data.columns:
            return {}
        
        market_cap = self.company_data['Market Capitalization'].dropna()
        return {
            'mean': market_cap.mean(),
            'median': market_cap.median(),
            'std': market_cap.std(),
            'min': market_cap.min(),
            'max': market_cap.max()
        }
    
    def _get_compensation_stats(self) -> Dict:
        """Get compensation statistics"""
        if 'Total Compensation' not in self.officers_data.columns:
            return {}
        
        comp = self.officers_data['Total Compensation'].dropna()
        return {
            'mean': comp.mean(),
            'median': comp.median(),
            'std': comp.std(),
            'min': comp.min(),
            'max': comp.max()
        }
    
    def _get_actions_date_range(self) -> Dict:
        """Get date range for corporate actions"""
        if 'Event_Date' not in self.corporate_actions.columns:
            return {}
        
        dates = self.corporate_actions['Event_Date'].dropna()
        if len(dates) == 0:
            return {}
        
        return {
            'earliest': dates.min(),
            'latest': dates.max(),
            'span_days': (dates.max() - dates.min()).days
        }
    
    def analyze_financial_metrics(self):
        """Analyze financial metrics"""
        logger.info("Analyzing financial metrics...")
        
        if self.company_data is None:
            return
        
        # Financial ratios analysis
        financial_metrics = {}
        
        # P/E Ratio analysis
        if 'Trailing Price-to-Earnings Ratio' in self.company_data.columns:
            pe_ratio = self.company_data['Trailing Price-to-Earnings Ratio'].dropna()
            financial_metrics['pe_ratio'] = {
                'mean': pe_ratio.mean(),
                'median': pe_ratio.median(),
                'high_pe_companies': len(pe_ratio[pe_ratio > 30]),
                'low_pe_companies': len(pe_ratio[pe_ratio < 15])
            }
        
        # Debt-to-Equity analysis
        if 'Debt-to-Equity Ratio' in self.company_data.columns:
            de_ratio = self.company_data['Debt-to-Equity Ratio'].dropna()
            financial_metrics['debt_equity'] = {
                'mean': de_ratio.mean(),
                'median': de_ratio.median(),
                'high_debt_companies': len(de_ratio[de_ratio > 1.0])
            }
        
        # Profitability analysis
        profit_cols = ['Profit Margin Percentage', 'Return on Assets', 'Return on Equity']
        for col in profit_cols:
            if col in self.company_data.columns:
                data = self.company_data[col].dropna()
                financial_metrics[col.lower().replace(' ', '_')] = {
                    'mean': data.mean(),
                    'median': data.median(),
                    'positive_companies': len(data[data > 0])
                }
        
        self.analysis_results['financial_metrics'] = financial_metrics
    
    def analyze_sector_performance(self):
        """Analyze performance by sector"""
        logger.info("Analyzing sector performance...")
        
        if self.company_data is None or 'Sector Display Name' not in self.company_data.columns:
            return
        
        sector_analysis = {}
        sectors = self.company_data['Sector Display Name'].dropna().unique()
        
        for sector in sectors:
            sector_data = self.company_data[self.company_data['Sector Display Name'] == sector]
            
            sector_metrics = {
                'company_count': len(sector_data),
                'avg_market_cap': sector_data['Market Capitalization'].mean() if 'Market Capitalization' in sector_data.columns else None,
                'avg_pe_ratio': sector_data['Trailing Price-to-Earnings Ratio'].mean() if 'Trailing Price-to-Earnings Ratio' in sector_data.columns else None,
                'avg_profit_margin': sector_data['Profit Margin Percentage'].mean() if 'Profit Margin Percentage' in sector_data.columns else None
            }
            
            sector_analysis[sector] = sector_metrics
        
        self.analysis_results['sector_performance'] = sector_analysis
    
    def analyze_executive_compensation(self):
        """Analyze executive compensation patterns"""
        logger.info("Analyzing executive compensation...")
        
        if self.officers_data is None:
            return
        
        compensation_analysis = {}
        
        # Overall compensation statistics
        if 'Total Compensation' in self.officers_data.columns:
            comp_data = self.officers_data['Total Compensation'].dropna()
            compensation_analysis['overall'] = {
                'mean': comp_data.mean(),
                'median': comp_data.median(),
                'top_10_percent': comp_data.quantile(0.9),
                'bottom_10_percent': comp_data.quantile(0.1)
            }
        
        # Compensation by title
        if 'Officer Title' in self.officers_data.columns:
            comp_by_title = self.officers_data.groupby('Officer Title')['Total Compensation'].agg([
                'mean', 'median', 'count'
            ]).round(2)
            compensation_analysis['by_title'] = comp_by_title.to_dict()
        
        # Age vs compensation correlation
        if all(col in self.officers_data.columns for col in ['Officer Age', 'Total Compensation']):
            age_comp_corr = self.officers_data['Officer Age'].corr(
                self.officers_data['Total Compensation']
            )
            compensation_analysis['age_correlation'] = age_comp_corr
        
        self.analysis_results['executive_compensation'] = compensation_analysis
    
    def analyze_corporate_actions_trends(self):
        """Analyze corporate actions trends"""
        logger.info("Analyzing corporate actions trends...")
        
        if self.corporate_actions is None:
            return
        
        actions_analysis = {}
        
        # Actions by type
        if 'Action Type' in self.corporate_actions.columns:
            actions_analysis['by_type'] = self.corporate_actions['Action Type'].value_counts().to_dict()
        
        # Dividend analysis
        if 'Amount' in self.corporate_actions.columns:
            dividend_data = self.corporate_actions[
                self.corporate_actions['Action Type'] == 'Dividend'
            ]['Amount'].dropna()
            
            if len(dividend_data) > 0:
                actions_analysis['dividend_stats'] = {
                    'mean': dividend_data.mean(),
                    'median': dividend_data.median(),
                    'total_companies_paying': len(dividend_data)
                }
        
        # Timeline analysis
        if 'Event_Date' in self.corporate_actions.columns:
            self.corporate_actions['Year'] = self.corporate_actions['Event_Date'].dt.year
            yearly_actions = self.corporate_actions['Year'].value_counts().sort_index().to_dict()
            actions_analysis['yearly_trend'] = yearly_actions
        
        self.analysis_results['corporate_actions'] = actions_analysis
    
    def create_visualizations(self):
        """Create comprehensive visualizations"""
        logger.info("Creating visualizations...")
        
        # Set up the figure directory
        if not os.path.exists('analysis_output'):
            os.makedirs('analysis_output')
        
        # Company data visualizations
        if self.company_data is not None:
            self._create_company_visualizations()
        
        # Officers data visualizations
        if self.officers_data is not None:
            self._create_officers_visualizations()
        
        # Corporate actions visualizations
        if self.corporate_actions is not None:
            self._create_actions_visualizations()
    
    def _create_company_visualizations(self):
        """Create company data visualizations"""
        
        # Market capitalization distribution
        if 'Market Capitalization' in self.company_data.columns:
            plt.figure(figsize=(12, 6))
            market_cap_data = self.company_data['Market Capitalization'].dropna()
            
            plt.subplot(1, 2, 1)
            plt.hist(market_cap_data, bins=30, alpha=0.7, color='skyblue')
            plt.title('Market Capitalization Distribution')
            plt.xlabel('Market Cap')
            plt.ylabel('Frequency')
            plt.yscale('log')
            
            plt.subplot(1, 2, 2)
            plt.boxplot(market_cap_data)
            plt.title('Market Cap Box Plot')
            plt.ylabel('Market Cap')
            
            plt.tight_layout()
            plt.savefig('analysis_output/market_cap_distribution.png', dpi=300, bbox_inches='tight')
            plt.show()
        
        # Sector distribution
        if 'Sector Display Name' in self.company_data.columns:
            plt.figure(figsize=(12, 8))
            sector_counts = self.company_data['Sector Display Name'].value_counts()
            
            plt.pie(sector_counts.values, labels=sector_counts.index, autopct='%1.1f%%')
            plt.title('Company Distribution by Sector')
            plt.savefig('analysis_output/sector_distribution.png', dpi=300, bbox_inches='tight')
            plt.show()
        
        # Financial ratios comparison
        ratio_columns = ['Trailing Price-to-Earnings Ratio', 'Price-to-Book Ratio', 'Debt-to-Equity Ratio']
        available_ratios = [col for col in ratio_columns if col in self.company_data.columns]
        
        if available_ratios:
            fig, axes = plt.subplots(1, len(available_ratios), figsize=(15, 5))
            if len(available_ratios) == 1:
                axes = [axes]
            
            for i, ratio in enumerate(available_ratios):
                data = self.company_data[ratio].dropna()
                axes[i].hist(data, bins=20, alpha=0.7)
                axes[i].set_title(f'{ratio} Distribution')
                axes[i].set_xlabel(ratio)
                axes[i].set_ylabel('Frequency')
            
            plt.tight_layout()
            plt.savefig('analysis_output/financial_ratios.png', dpi=300, bbox_inches='tight')
            plt.show()
    
    def _create_officers_visualizations(self):
        """Create officers data visualizations"""
        
        # Compensation distribution
        if 'Total Compensation' in self.officers_data.columns:
            plt.figure(figsize=(12, 6))
            comp_data = self.officers_data['Total Compensation'].dropna()
            
            plt.subplot(1, 2, 1)
            plt.hist(comp_data, bins=30, alpha=0.7, color='lightcoral')
            plt.title('Executive Compensation Distribution')
            plt.xlabel('Total Compensation')
            plt.ylabel('Frequency')
            
            plt.subplot(1, 2, 2)
            plt.boxplot(comp_data)
            plt.title('Compensation Box Plot')
            plt.ylabel('Total Compensation')
            
            plt.tight_layout()
            plt.savefig('analysis_output/compensation_distribution.png', dpi=300, bbox_inches='tight')
            plt.show()
        
        # Age distribution
        if 'Officer Age' in self.officers_data.columns:
            plt.figure(figsize=(10, 6))
            age_data = self.officers_data['Officer Age'].dropna()
            
            plt.hist(age_data, bins=20, alpha=0.7, color='lightgreen')
            plt.title('Executive Age Distribution')
            plt.xlabel('Age')
            plt.ylabel('Frequency')
            plt.axvline(age_data.mean(), color='red', linestyle='--', label=f'Mean: {age_data.mean():.1f}')
            plt.legend()
            
            plt.savefig('analysis_output/age_distribution.png', dpi=300, bbox_inches='tight')
            plt.show()
        
        # Compensation by title (if enough data)
        if 'Officer Title' in self.officers_data.columns and 'Total Compensation' in self.officers_data.columns:
            title_comp = self.officers_data.groupby('Officer Title')['Total Compensation'].mean().sort_values(ascending=False)
            
            if len(title_comp) > 1:
                plt.figure(figsize=(12, 8))
                title_comp.plot(kind='bar')
                plt.title('Average Compensation by Title')
                plt.xlabel('Officer Title')
                plt.ylabel('Average Total Compensation')
                plt.xticks(rotation=45, ha='right')
                plt.tight_layout()
                plt.savefig('analysis_output/compensation_by_title.png', dpi=300, bbox_inches='tight')
                plt.show()
    
    def _create_actions_visualizations(self):
        """Create corporate actions visualizations"""
        
        # Actions by type
        if 'Action Type' in self.corporate_actions.columns:
            plt.figure(figsize=(10, 6))
            action_counts = self.corporate_actions['Action Type'].value_counts()
            
            plt.bar(action_counts.index, action_counts.values, color='gold')
            plt.title('Corporate Actions by Type')
            plt.xlabel('Action Type')
            plt.ylabel('Count')
            plt.xticks(rotation=45)
            
            plt.tight_layout()
            plt.savefig('analysis_output/actions_by_type.png', dpi=300, bbox_inches='tight')
            plt.show()
        
        # Timeline of actions
        if 'Event_Date' in self.corporate_actions.columns:
            plt.figure(figsize=(12, 6))
            self.corporate_actions['Year'] = self.corporate_actions['Event_Date'].dt.year
            yearly_counts = self.corporate_actions['Year'].value_counts().sort_index()
            
            plt.plot(yearly_counts.index, yearly_counts.values, marker='o', linewidth=2)
            plt.title('Corporate Actions Timeline')
            plt.xlabel('Year')
            plt.ylabel('Number of Actions')
            plt.grid(True, alpha=0.3)
            
            plt.tight_layout()
            plt.savefig('analysis_output/actions_timeline.png', dpi=300, bbox_inches='tight')
            plt.show()
    
    def generate_comprehensive_report(self) -> str:
        """Generate comprehensive analysis report"""
        logger.info("Generating comprehensive report...")
        
        report = []
        report.append("=" * 80)
        report.append("COMPREHENSIVE COMPANY DATA ANALYSIS REPORT")
        report.append("=" * 80)
        report.append(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        # Data Overview Section
        if 'overview' in self.analysis_results:
            report.append("DATA OVERVIEW")
            report.append("-" * 40)
            
            overview = self.analysis_results['overview']
            
            if overview.get('company_data'):
                cd = overview['company_data']
                report.append(f"Total Companies: {cd.get('total_companies', 'N/A')}")
                report.append(f"Unique Sectors: {cd.get('unique_sectors', 'N/A')}")
                report.append(f"Unique Industries: {cd.get('unique_industries', 'N/A')}")
                
                if cd.get('market_cap_stats'):
                    mcs = cd['market_cap_stats']
                    report.append(f"Average Market Cap: ${mcs.get('mean', 0):,.0f}")
                    report.append(f"Median Market Cap: ${mcs.get('median', 0):,.0f}")
            
            if overview.get('officers_data'):
                od = overview['officers_data']
                report.append(f"Total Officers: {od.get('total_officers', 'N/A')}")
                report.append(f"Average Age: {od.get('avg_age', 'N/A'):.1f}" if od.get('avg_age') else "Average Age: N/A")
                
                if od.get('compensation_stats'):
                    cs = od['compensation_stats']
                    report.append(f"Average Compensation: ${cs.get('mean', 0):,.0f}")
            
            if overview.get('corporate_actions'):
                ca = overview['corporate_actions']
                report.append(f"Total Corporate Actions: {ca.get('total_actions', 'N/A')}")
            
            report.append("")
        
        # Financial Metrics Section
        if 'financial_metrics' in self.analysis_results:
            report.append("FINANCIAL METRICS ANALYSIS")
            report.append("-" * 40)
            
            fm = self.analysis_results['financial_metrics']
            
            if 'pe_ratio' in fm:
                pe = fm['pe_ratio']
                report.append(f"Average P/E Ratio: {pe.get('mean', 0):.2f}")
                report.append(f"High P/E Companies (>30): {pe.get('high_pe_companies', 0)}")
                report.append(f"Low P/E Companies (<15): {pe.get('low_pe_companies', 0)}")
            
            report.append("")
        
        # Sector Performance Section
        if 'sector_performance' in self.analysis_results:
            report.append("SECTOR PERFORMANCE")
            report.append("-" * 40)
            
            sp = self.analysis_results['sector_performance']
            for sector, metrics in sp.items():
                report.append(f"{sector}:")
                report.append(f"  Companies: {metrics.get('company_count', 0)}")
                if metrics.get('avg_market_cap'):
                    report.append(f"  Avg Market Cap: ${metrics['avg_market_cap']:,.0f}")
                if metrics.get('avg_pe_ratio'):
                    report.append(f"  Avg P/E Ratio: {metrics['avg_pe_ratio']:.2f}")
                report.append("")
        
        # Executive Compensation Section
        if 'executive_compensation' in self.analysis_results:
            report.append("EXECUTIVE COMPENSATION ANALYSIS")
            report.append("-" * 40)
            
            ec = self.analysis_results['executive_compensation']
            if 'overall' in ec:
                overall = ec['overall']
                report.append(f"Average Compensation: ${overall.get('mean', 0):,.0f}")
                report.append(f"Median Compensation: ${overall.get('median', 0):,.0f}")
                report.append(f"Top 10% Threshold: ${overall.get('top_10_percent', 0):,.0f}")
            
            report.append("")
        
        # Recommendations
        report.append("RECOMMENDATIONS & INSIGHTS")
        report.append("-" * 40)
        report.extend(self._generate_recommendations())
        
        # Save report
        report_text = "\n".join(report)
        with open('analysis_output/comprehensive_report.txt', 'w') as f:
            f.write(report_text)
        
        return report_text
    
    def _generate_recommendations(self) -> List[str]:
        """Generate analysis-based recommendations"""
        recommendations = []
        
        if 'financial_metrics' in self.analysis_results:
            fm = self.analysis_results['financial_metrics']
            
            if 'pe_ratio' in fm:
                high_pe = fm['pe_ratio'].get('high_pe_companies', 0)
                if high_pe > 0:
                    recommendations.append(f"• {high_pe} companies have high P/E ratios (>30), indicating potential overvaluation or high growth expectations")
        
        if 'sector_performance' in self.analysis_results:
            recommendations.append("• Consider sector diversification based on performance metrics")
        
        if 'executive_compensation' in self.analysis_results:
            ec = self.analysis_results['executive_compensation']
            if 'overall' in ec:
                recommendations.append("• Executive compensation analysis shows significant variation - consider benchmarking against industry standards")
        
        if not recommendations:
            recommendations.append("• Conduct deeper analysis with more data points for meaningful insights")
        
        return recommendations
    
    def export_results(self, format_type: str = 'excel'):
        """Export analysis results"""
        logger.info(f"Exporting results in {format_type} format...")
        
        if format_type.lower() == 'excel':
            with pd.ExcelWriter('analysis_output/analysis_results.xlsx') as writer:
                # Export summary statistics
                if self.analysis_results.get('overview'):
                    overview_df = pd.DataFrame([self.analysis_results['overview']])
                    overview_df.to_excel(writer, sheet_name='Overview', index=False)
                
                # Export processed data
                if self.company_data is not None:
                    self.company_data.to_excel(writer, sheet_name='Company_Data', index=False)
                
                if self.officers_data is not None:
                    self.officers_data.to_excel(writer, sheet_name='Officers_Data', index=False)
                
                if self.corporate_actions is not None:
                    self.corporate_actions.to_excel(writer, sheet_name='Corporate_Actions', index=False)
    
    def run_complete_analysis(self, excel_file_path: str = None):
        """Run the complete analysis pipeline"""
        if excel_file_path:
            self.excel_file_path = excel_file_path
        
        print("Starting comprehensive data analysis...")
        print("=" * 50)
        
        # Load data
        if not self.load_data():
            print("Failed to load data. Please check the file path and format.")
            return
        
        # Clean and prepare data
        self.clean_and_prepare_data()
        
        # Generate overview
        overview = self.generate_data_overview()
        
        # Run analyses
        self.analyze_financial_metrics()
        self.analyze_sector_performance()
        self.analyze_executive_compensation()
        self.analyze_corporate_actions_trends()
        
        # Create visualizations
        self.create_visualizations()
        
        # Generate report
        report = self.generate_comprehensive_report()
        
        # Export results
        self.export_results()
        
        print("\nAnalysis Complete!")
        print("Results saved to 'analysis_output' directory")
        print("- Visualizations: PNG files")
        print("- Comprehensive report: comprehensive_report.txt")
        print("- Processed data: analysis_results.xlsx")
        
        return self.analysis_results

# Interactive Analysis Function
def run_interactive_analysis():
    """Run interactive analysis with user input"""
    print("Welcome to the Company Data Analyzer!")
    print("=" * 50)
    
    # Get file path from user
    while True:
        file_path = input("Enter the path to your Excel file: ").strip()
        if os.path.exists(file_path):
            break
        print("File not found. Please check the path and try again.")
    
    # Initialize analyzer
    analyzer = CompanyDataAnalyzer(file_path)
    
    # Run analysis
    results = analyzer.run_complete_analysis()
    
    # Interactive menu for additional analyses
    while True:
        print("\nAdditional Analysis Options:")
        print("1. View detailed financial metrics")
        print("2. Compare sector performance")
        print("3. Executive compensation deep dive")
        print("4. Corporate actions timeline analysis")
        print("5. Custom data filtering and analysis")
        print("6. Generate custom visualization")
        print("7. Export specific dataset")
        print("8. Exit")
        
        choice = input("\nSelect an option (1-8): ").strip()
        
        if choice == '1':
            display_financial_metrics(analyzer)
        elif choice == '2':
            compare_sectors(analyzer)
        elif choice == '3':
            compensation_deep_dive(analyzer)
        elif choice == '4':
            actions_timeline_analysis(analyzer)
        elif choice == '5':
            custom_analysis(analyzer)
        elif choice == '6':
            create_custom_visualization(analyzer)
        elif choice == '7':
            export_custom_dataset(analyzer)
        elif choice == '8':
            print("Thank you for using the Company Data Analyzer!")
            break
        else:
            print("Invalid choice. Please try again.")

def display_financial_metrics(analyzer):
    """Display detailed financial metrics"""
    if 'financial_metrics' in analyzer.analysis_results:
        fm = analyzer.analysis_results['financial_metrics']
        print("\nDetailed Financial Metrics:")
        print("-" * 30)
        
        for metric, values in fm.items():
            print(f"\n{metric.replace('_', ' ').title()}:")
            for key, value in values.items():
                if isinstance(value, (int, float)):
                    print(f"  {key.replace('_', ' ').title()}: {value:,.2f}")
                else:
                    print(f"  {key.replace('_', ' ').title()}: {value}")

def compare_sectors(analyzer):
    """Compare sector performance"""
    if 'sector_performance' in analyzer.analysis_results:
        sp = analyzer.analysis_results['sector_performance']
        print("\nSector Performance Comparison:")
        print("-" * 40)
        
        # Create comparison DataFrame
        comparison_data = []
        for sector, metrics in sp.items():
            comparison_data.append({
                'Sector': sector,
                'Companies': metrics.get('company_count', 0),
                'Avg Market Cap': metrics.get('avg_market_cap', 0),
                'Avg P/E Ratio': metrics.get('avg_pe_ratio', 0),
                'Avg Profit Margin': metrics.get('avg_profit_margin', 0)
            })
        
        df = pd.DataFrame(comparison_data)
        print(df.to_string(index=False))
        
        # Create visualization
        if len(df) > 0:
            fig, axes = plt.subplots(2, 2, figsize=(15, 10))
            
            # Market cap comparison
            if df['Avg Market Cap'].notna().any():
                axes[0, 0].bar(df['Sector'], df['Avg Market Cap'])
                axes[0, 0].set_title('Average Market Cap by Sector')
                axes[0, 0].tick_params(axis='x', rotation=45)
            
            # P/E ratio comparison
            if df['Avg P/E Ratio'].notna().any():
                axes[0, 1].bar(df['Sector'], df['Avg P/E Ratio'])
                axes[0, 1].set_title('Average P/E Ratio by Sector')
                axes[0, 1].tick_params(axis='x', rotation=45)
            
            # Company count
            axes[1, 0].bar(df['Sector'], df['Companies'])
            axes[1, 0].set_title('Number of Companies by Sector')
            axes[1, 0].tick_params(axis='x', rotation=45)
            
            # Profit margin comparison
            if df['Avg Profit Margin'].notna().any():
                axes[1, 1].bar(df['Sector'], df['Avg Profit Margin'])
                axes[1, 1].set_title('Average Profit Margin by Sector')
                axes[1, 1].tick_params(axis='x', rotation=45)
            
            plt.tight_layout()
            plt.savefig('analysis_output/sector_comparison.png', dpi=300, bbox_inches='tight')
            plt.show()

def compensation_deep_dive(analyzer):
    """Deep dive into compensation analysis"""
    if analyzer.officers_data is None:
        print("No officers data available for compensation analysis.")
        return
    
    print("\nExecutive Compensation Deep Dive:")
    print("-" * 40)
    
    # Top compensated executives
    if 'Total Compensation' in analyzer.officers_data.columns:
        top_10 = analyzer.officers_data.nlargest(10, 'Total Compensation')
        print("\nTop 10 Compensated Executives:")
        print(top_10[['Officer Name', 'Officer Title', 'Total Compensation']].to_string(index=False))
    
    # Compensation by age group
    if all(col in analyzer.officers_data.columns for col in ['Officer Age', 'Total Compensation']):
        analyzer.officers_data['Age_Group'] = pd.cut(
            analyzer.officers_data['Officer Age'], 
            bins=[0, 40, 50, 60, 100], 
            labels=['<40', '40-50', '50-60', '>60']
        )
        
        age_comp = analyzer.officers_data.groupby('Age_Group')['Total Compensation'].agg([
            'mean', 'median', 'count'
        ]).round(2)
        
        print("\nCompensation by Age Group:")
        print(age_comp.to_string())
        
        # Visualization
        plt.figure(figsize=(10, 6))
        age_comp['mean'].plot(kind='bar')
        plt.title('Average Compensation by Age Group')
        plt.xlabel('Age Group')
        plt.ylabel('Average Compensation')
        plt.xticks(rotation=0)
        plt.tight_layout()
        plt.savefig('analysis_output/compensation_by_age.png', dpi=300, bbox_inches='tight')
        plt.show()

def actions_timeline_analysis(analyzer):
    """Analyze corporate actions timeline"""
    if analyzer.corporate_actions is None:
        print("No corporate actions data available.")
        return
    
    print("\nCorporate Actions Timeline Analysis:")
    print("-" * 40)
    
    if 'Event_Date' in analyzer.corporate_actions.columns:
        # Monthly distribution
        analyzer.corporate_actions['Year_Month'] = analyzer.corporate_actions['Event_Date'].dt.to_period('M')
        monthly_counts = analyzer.corporate_actions['Year_Month'].value_counts().sort_index()
        
        print(f"\nActions per month (last 12 months):")
        print(monthly_counts.tail(12).to_string())
        
        # Actions by type over time
        if 'Action Type' in analyzer.corporate_actions.columns:
            timeline_pivot = analyzer.corporate_actions.pivot_table(
                values='Ticker', 
                index='Year_Month', 
                columns='Action Type', 
                aggfunc='count', 
                fill_value=0
            )
            
            if len(timeline_pivot) > 0:
                plt.figure(figsize=(12, 6))
                timeline_pivot.plot(kind='line', marker='o')
                plt.title('Corporate Actions Timeline by Type')
                plt.xlabel('Time Period')
                plt.ylabel('Number of Actions')
                plt.legend(title='Action Type')
                plt.xticks(rotation=45)
                plt.tight_layout()
                plt.savefig('analysis_output/actions_timeline_detailed.png', dpi=300, bbox_inches='tight')
                plt.show()

def custom_analysis(analyzer):
    """Perform custom analysis based on user input"""
    print("\nCustom Analysis Options:")
    print("1. Filter companies by market cap range")
    print("2. Filter by sector and analyze metrics")
    print("3. Find companies with specific financial criteria")
    print("4. Compare specific companies")
    
    choice = input("Select option (1-4): ").strip()
    
    if choice == '1':
        filter_by_market_cap(analyzer)
    elif choice == '2':
        analyze_by_sector(analyzer)
    elif choice == '3':
        find_companies_by_criteria(analyzer)
    elif choice == '4':
        compare_specific_companies(analyzer)

def filter_by_market_cap(analyzer):
    """Filter companies by market cap range"""
    if analyzer.company_data is None or 'Market Capitalization' not in analyzer.company_data.columns:
        print("Market capitalization data not available.")
        return
    
    try:
        min_cap = float(input("Enter minimum market cap (in millions): ")) * 1e6
        max_cap = float(input("Enter maximum market cap (in millions): ")) * 1e6
        
        filtered = analyzer.company_data[
            (analyzer.company_data['Market Capitalization'] >= min_cap) &
            (analyzer.company_data['Market Capitalization'] <= max_cap)
        ]
        
        print(f"\nFound {len(filtered)} companies in the specified range:")
        if len(filtered) > 0:
            display_cols = ['Ticker', 'Full Company Name', 'Market Capitalization', 'Sector Display Name']
            available_cols = [col for col in display_cols if col in filtered.columns]
            print(filtered[available_cols].to_string(index=False))
        
    except ValueError:
        print("Invalid input. Please enter numeric values.")

def analyze_by_sector(analyzer):
    """Analyze specific sector"""
    if analyzer.company_data is None or 'Sector Display Name' not in analyzer.company_data.columns:
        print("Sector data not available.")
        return
    
    sectors = analyzer.company_data['Sector Display Name'].dropna().unique()
    print("\nAvailable sectors:")
    for i, sector in enumerate(sectors, 1):
        print(f"{i}. {sector}")
    
    try:
        choice = int(input("Select sector number: ")) - 1
        if 0 <= choice < len(sectors):
            selected_sector = sectors[choice]
            sector_data = analyzer.company_data[
                analyzer.company_data['Sector Display Name'] == selected_sector
            ]
            
            print(f"\nAnalysis for {selected_sector} sector:")
            print(f"Number of companies: {len(sector_data)}")
            
            # Calculate metrics
            numeric_cols = sector_data.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) > 0:
                print("\nSector Metrics:")
                metrics = sector_data[numeric_cols].describe()
                print(metrics.to_string())
        else:
            print("Invalid selection.")
            
    except ValueError:
        print("Invalid input. Please enter a number.")

def find_companies_by_criteria(analyzer):
    """Find companies matching specific criteria"""
    if analyzer.company_data is None:
        print("Company data not available.")
        return
    
    print("\nFinancial Criteria Options:")
    print("1. High P/E ratio (>25)")
    print("2. Low P/E ratio (<15)")
    print("3. High dividend yield (>4%)")
    print("4. High profit margin (>15%)")
    print("5. Low debt-to-equity (<0.5)")
    
    choice = input("Select criteria (1-5): ").strip()
    
    filters = {
        '1': ('Trailing Price-to-Earnings Ratio', '>', 25),
        '2': ('Trailing Price-to-Earnings Ratio', '<', 15),
        '3': ('Dividend Yield Percentage', '>', 4),
        '4': ('Profit Margin Percentage', '>', 15),
        '5': ('Debt-to-Equity Ratio', '<', 0.5)
    }
    
    if choice in filters:
        col, op, threshold = filters[choice]
        if col in analyzer.company_data.columns:
            if op == '>':
                filtered = analyzer.company_data[analyzer.company_data[col] > threshold]
            else:
                filtered = analyzer.company_data[analyzer.company_data[col] < threshold]
            
            print(f"\nCompanies matching criteria ({col} {op} {threshold}):")
            if len(filtered) > 0:
                display_cols = ['Ticker', 'Full Company Name', col, 'Sector Display Name']
                available_cols = [col for col in display_cols if col in filtered.columns]
                print(filtered[available_cols].to_string(index=False))
            else:
                print("No companies match the specified criteria.")
        else:
            print(f"Column {col} not available in the data.")

def compare_specific_companies(analyzer):
    """Compare specific companies"""
    if analyzer.company_data is None:
        print("Company data not available.")
        return
    
    available_tickers = analyzer.company_data['Ticker'].dropna().unique()
    print(f"\nAvailable tickers: {', '.join(available_tickers[:20])}...")
    
    tickers_input = input("Enter ticker symbols separated by commas: ").strip().upper()
    tickers = [t.strip() for t in tickers_input.split(',')]
    
    comparison_data = analyzer.company_data[
        analyzer.company_data['Ticker'].isin(tickers)
    ]
    
    if len(comparison_data) > 0:
        print(f"\nComparison of {len(comparison_data)} companies:")
        
        # Select key comparison metrics
        comparison_cols = [
            'Ticker', 'Full Company Name', 'Market Capitalization', 
            'Trailing Price-to-Earnings Ratio', 'Profit Margin Percentage',
            'Debt-to-Equity Ratio', 'Dividend Yield Percentage'
        ]
        
        available_cols = [col for col in comparison_cols if col in comparison_data.columns]
        print(comparison_data[available_cols].to_string(index=False))
        
        # Create comparison chart
        numeric_cols = [col for col in available_cols if col != 'Ticker' and col != 'Full Company Name']
        if len(numeric_cols) > 1:
            comparison_data_clean = comparison_data[['Ticker'] + numeric_cols].set_index('Ticker')
            
            # Normalize data for comparison
            normalized_data = comparison_data_clean.div(comparison_data_clean.max())
            
            plt.figure(figsize=(12, 8))
            normalized_data.T.plot(kind='bar')
            plt.title('Company Comparison (Normalized Metrics)')
            plt.xlabel('Metrics')
            plt.ylabel('Normalized Values')
            plt.legend(title='Companies')
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.savefig('analysis_output/company_comparison.png', dpi=300, bbox_inches='tight')
            plt.show()
    else:
        print("No companies found with the specified tickers.")

def create_custom_visualization(analyzer):
    """Create custom visualization"""
    print("\nCustom Visualization Options:")
    print("1. Scatter plot (two numeric variables)")
    print("2. Correlation heatmap")
    print("3. Distribution comparison")
    print("4. Time series plot")
    
    choice = input("Select visualization type (1-4): ").strip()
    
    if choice == '1':
        create_scatter_plot(analyzer)
    elif choice == '2':
        create_correlation_heatmap(analyzer)
    elif choice == '3':
        create_distribution_comparison(analyzer)
    elif choice == '4':
        create_time_series_plot(analyzer)

def create_scatter_plot(analyzer):
    """Create scatter plot"""
    if analyzer.company_data is None:
        print("Company data not available.")
        return
    
    numeric_cols = analyzer.company_data.select_dtypes(include=[np.number]).columns.tolist()
    print(f"\nAvailable numeric columns: {', '.join(numeric_cols[:10])}...")
    
    x_col = input("Enter X-axis column name: ").strip()
    y_col = input("Enter Y-axis column name: ").strip()
    
    if x_col in analyzer.company_data.columns and y_col in analyzer.company_data.columns:
        plt.figure(figsize=(10, 6))
        
        # Remove NaN values
        plot_data = analyzer.company_data[[x_col, y_col]].dropna()
        
        plt.scatter(plot_data[x_col], plot_data[y_col], alpha=0.6)
        plt.xlabel(x_col)
        plt.ylabel(y_col)
        plt.title(f'{x_col} vs {y_col}')
        
        # Add correlation coefficient
        corr = plot_data[x_col].corr(plot_data[y_col])
        plt.text(0.05, 0.95, f'Correlation: {corr:.3f}', transform=plt.gca().transAxes, 
                bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.8))
        
        plt.tight_layout()
        plt.savefig('analysis_output/custom_scatter_plot.png', dpi=300, bbox_inches='tight')
        plt.show()
    else:
        print("Invalid column names.")

def create_correlation_heatmap(analyzer):
    """Create correlation heatmap"""
    if analyzer.company_data is None:
        print("Company data not available.")
        return
    
    numeric_data = analyzer.company_data.select_dtypes(include=[np.number])
    
    if len(numeric_data.columns) > 1:
        plt.figure(figsize=(12, 10))
        correlation_matrix = numeric_data.corr()
        
        # Create heatmap
        sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', center=0,
                   square=True, linewidths=0.5, cbar_kws={"shrink": .8}, fmt='.2f')
        
        plt.title('Correlation Matrix - Financial Metrics')
        plt.tight_layout()
        plt.savefig('analysis_output/correlation_heatmap.png', dpi=300, bbox_inches='tight')
        plt.show()
    else:
        print("Insufficient numeric data for correlation analysis.")

def create_distribution_comparison(analyzer):
    """Create distribution comparison"""
    if analyzer.company_data is None or 'Sector Display Name' not in analyzer.company_data.columns:
        print("Required data not available.")
        return
    
    numeric_cols = analyzer.company_data.select_dtypes(include=[np.number]).columns.tolist()
    print(f"\nAvailable numeric columns: {', '.join(numeric_cols[:10])}...")
    
    metric = input("Enter metric to compare across sectors: ").strip()
    
    if metric in analyzer.company_data.columns:
        plt.figure(figsize=(12, 6))
        
        # Create box plot comparing sectors
        sectors = analyzer.company_data['Sector Display Name'].dropna().unique()[:5]  # Limit to 5 sectors
        sector_data = analyzer.company_data[
            analyzer.company_data['Sector Display Name'].isin(sectors)
        ]
        
        sector_data.boxplot(column=metric, by='Sector Display Name', ax=plt.gca())
        plt.title(f'Distribution of {metric} by Sector')
        plt.suptitle('')  # Remove automatic title
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig('analysis_output/distribution_comparison.png', dpi=300, bbox_inches='tight')
        plt.show()
    else:
        print("Invalid metric name.")

def create_time_series_plot(analyzer):
    """Create time series plot"""
    if analyzer.corporate_actions is None or 'Event_Date' not in analyzer.corporate_actions.columns:
        print("Corporate actions data with dates not available.")
        return
    
    # Monthly actions count
    analyzer.corporate_actions['Year_Month'] = analyzer.corporate_actions['Event_Date'].dt.to_period('M')
    monthly_counts = analyzer.corporate_actions['Year_Month'].value_counts().sort_index()
    
    plt.figure(figsize=(12, 6))
    monthly_counts.plot(kind='line', marker='o')
    plt.title('Corporate Actions Over Time')
    plt.xlabel('Time Period')
    plt.ylabel('Number of Actions')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('analysis_output/time_series_plot.png', dpi=300, bbox_inches='tight')
    plt.show()

def export_custom_dataset(analyzer):
    """Export custom dataset"""
    print("\nExport Options:")
    print("1. Filtered company data")
    print("2. Summary statistics by sector")
    print("3. Top performers by metric")
    print("4. Custom query results")
    
    choice = input("Select export option (1-4): ").strip()
    
    if choice == '1':
        export_filtered_data(analyzer)
    elif choice == '2':
        export_sector_summary(analyzer)
    elif choice == '3':
        export_top_performers(analyzer)
    elif choice == '4':
        export_custom_query(analyzer)

def export_filtered_data(analyzer):
    """Export filtered company data"""
    if analyzer.company_data is None:
        print("Company data not available.")
        return
    
    # Get filter criteria
    print("Filter options:")
    print("1. Market cap range")
    print("2. Specific sector")
    print("3. P/E ratio range")
    
    filter_choice = input("Select filter (1-3): ").strip()
    
    filtered_data = analyzer.company_data.copy()
    filename_suffix = ""
    
    if filter_choice == '1':
        try:
            min_cap = float(input("Minimum market cap (millions): ")) * 1e6
            max_cap = float(input("Maximum market cap (millions): ")) * 1e6
            filtered_data = filtered_data[
                (filtered_data['Market Capitalization'] >= min_cap) &
                (filtered_data['Market Capitalization'] <= max_cap)
            ]
            filename_suffix = f"_marketcap_{min_cap/1e6:.0f}M_{max_cap/1e6:.0f}M"
        except:
            print("Invalid input.")
            return
    
    elif filter_choice == '2':
        if 'Sector Display Name' in analyzer.company_data.columns:
            sectors = analyzer.company_data['Sector Display Name'].dropna().unique()
            print("Available sectors:", ', '.join(sectors))
            sector = input("Enter sector name: ").strip()
            filtered_data = filtered_data[filtered_data['Sector Display Name'] == sector]
            filename_suffix = f"_{sector.replace(' ', '_')}"
        else:
            print("Sector data not available.")
            return
    
    elif filter_choice == '3':
        if 'Trailing Price-to-Earnings Ratio' in analyzer.company_data.columns:
            try:
                min_pe = float(input("Minimum P/E ratio: "))
                max_pe = float(input("Maximum P/E ratio: "))
                filtered_data = filtered_data[
                    (filtered_data['Trailing Price-to-Earnings Ratio'] >= min_pe) &
                    (filtered_data['Trailing Price-to-Earnings Ratio'] <= max_pe)
                ]
                filename_suffix = f"_pe_{min_pe}_{max_pe}"
            except:
                print("Invalid input.")
                return
        else:
            print("P/E ratio data not available.")
            return
    
    # Export filtered data
    filename = f"analysis_output/filtered_companies{filename_suffix}.xlsx"
    filtered_data.to_excel(filename, index=False)
    print(f"Exported {len(filtered_data)} companies to {filename}")

def export_sector_summary(analyzer):
    """Export sector summary statistics"""
    if analyzer.company_data is None or 'Sector Display Name' not in analyzer.company_data.columns:
        print("Required data not available.")
        return
    
    # Create sector summary
    numeric_cols = analyzer.company_data.select_dtypes(include=[np.number]).columns
    summary_data = []
    
    for sector in analyzer.company_data['Sector Display Name'].dropna().unique():
        sector_data = analyzer.company_data[analyzer.company_data['Sector Display Name'] == sector]
        
        summary_row = {'Sector': sector, 'Company_Count': len(sector_data)}
        
        for col in numeric_cols:
            if col in sector_data.columns:
                summary_row[f'{col}_Mean'] = sector_data[col].mean()
                summary_row[f'{col}_Median'] = sector_data[col].median()
        
        summary_data.append(summary_row)
    
    summary_df = pd.DataFrame(summary_data)
    filename = "analysis_output/sector_summary.xlsx"
    summary_df.to_excel(filename, index=False)
    print(f"Sector summary exported to {filename}")

def export_top_performers(analyzer):
    """Export top performers by specific metric"""
    if analyzer.company_data is None:
        print("Company data not available.")
        return
    
    numeric_cols = analyzer.company_data.select_dtypes(include=[np.number]).columns.tolist()
    print(f"Available metrics: {', '.join(numeric_cols[:10])}...")
    
    metric = input("Enter metric for ranking: ").strip()
    
    if metric in analyzer.company_data.columns:
        try:
            n = int(input("Number of top performers to export (default 20): ") or "20")
            
            top_performers = analyzer.company_data.nlargest(n, metric)
            filename = f"analysis_output/top_{n}_{metric.replace(' ', '_')}.xlsx"
            top_performers.to_excel(filename, index=False)
            print(f"Top {n} performers by {metric} exported to {filename}")
        except:
            print("Invalid input.")
    else:
        print("Invalid metric name.")

def export_custom_query(analyzer):
    """Export results of custom query"""
    print("Custom query functionality would require more complex implementation.")
    print("For now, use the other export options or modify the code directly.")

# Additional utility functions
class DataValidator:
    """Utility class for data validation"""
    
    @staticmethod
    def validate_numeric_data(df: pd.DataFrame, column: str) -> bool:
        """Validate if column contains numeric data"""
        return column in df.columns and pd.api.types.is_numeric_dtype(df[column])
    
    @staticmethod
    def check_data_quality(df: pd.DataFrame) -> Dict:
        """Check overall data quality"""
        quality_metrics = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'missing_data_percentage': (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100,
            'duplicate_rows': df.duplicated().sum(),
            'numeric_columns': len(df.select_dtypes(include=[np.number]).columns),
            'text_columns': len(df.select_dtypes(include=['object']).columns)
        }
        return quality_metrics

# Performance optimization utilities
def optimize_dataframe_memory(df: pd.DataFrame) -> pd.DataFrame:
    """Optimize DataFrame memory usage"""
    optimized_df = df.copy()
    
    # Convert object columns to category if they have limited unique values
    for col in optimized_df.select_dtypes(include=['object']).columns:
        if optimized_df[col].nunique() / len(optimized_df) < 0.1:  # Less than 10% unique values
            optimized_df[col] = optimized_df[col].astype('category')
    
    # Optimize numeric types
    for col in optimized_df.select_dtypes(include=['int64']).columns:
        if optimized_df[col].max() < 2147483647:  # int32 max
            optimized_df[col] = optimized_df[col].astype('int32')
    
    for col in optimized_df.select_dtypes(include=['float64']).columns:
        optimized_df[col] = optimized_df[col].astype('float32')
    
    return optimized_df

# Main execution
if __name__ == "__main__":
    print("Company Data Analysis Program")
    print("=" * 50)
    print("Choose execution mode:")
    print("1. Interactive Analysis (guided)")
    print("2. Batch Analysis (automated)")
    print("3. Custom Script Mode")
    
    mode = input("Select mode (1-3): ").strip()
    
    if mode == '1':
        run_interactive_analysis()
    elif mode == '2':
        file_path = input("Enter Excel file path: ").strip()
        if os.path.exists(file_path):
            analyzer = CompanyDataAnalyzer(file_path)
            analyzer.run_complete_analysis()
        else:
            print("File not found.")
    elif mode == '3':
        print("Custom Script Mode - Modify the code as needed")
        # Example custom analysis
        file_path = "All_Tickers.xlsx"  # Replace with actual path
        if os.path.exists(file_path):
            analyzer = CompanyDataAnalyzer(file_path)
            analyzer.run_complete_analysis()
    else:
        print("Invalid selection. Running interactive mode by default.")
        run_interactive_analysis()