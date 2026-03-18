"""
AI Data Engineering Pipeline - Main Application
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Prompt, Confirm

from config.config import Config
from src.database.db_manager import DatabaseManager
from src.ingestion.fallback_manager import FallbackManager
from src.processing.cleaner import DataCleaner
from src.processing.validator import DataValidator
from src.intelligence.anomaly_detector import AnomalyDetector
from src.intelligence.anomaly_fixer import AnomalyFixer
from src.nlp.query_parser import QueryParser
from src.nlp.grammar_corrector import GrammarCorrector
from src.nlp.query_executor import QueryExecutor
from src.analytics.statistics import StatisticsAnalyzer
from src.analytics.visualizer import Visualizer
from src.monitoring.logger import logger


class DataPipeline:
    """Main pipeline application"""
    
    def __init__(self):
        self.console = Console()
        self.db = DatabaseManager()
        self.fallback_manager = FallbackManager()
        self.cleaner = DataCleaner()
        self.validator = DataValidator()
        self.anomaly_detector = AnomalyDetector()
        self.anomaly_fixer = AnomalyFixer()
        self.stats_analyzer = StatisticsAnalyzer()
        self.visualizer = Visualizer()
        
        self.current_df = None
        self.current_table = None
        self.data_mode = 'csv'
        
        Config.create_directories()
        Config.validate_config()
    
    def run(self):
        """Run the main application loop"""
        self.display_welcome()
        
        while True:
            try:
                self.display_menu()
                choice = Prompt.ask("Select an option", default="1")
                
                if choice == '1':
                    self.upload_csv()
                elif choice == '2':
                    self.query_data()
                elif choice == '3':
                    self.view_statistics()
                elif choice == '4':
                    self.create_visualizations()
                elif choice == '5':
                    self.view_data_quality()
                elif choice == '0':
                    self.console.print("\n[bold green]Goodbye![/bold green]\n")
                    break
                else:
                    self.console.print("[red]Invalid option[/red]")
                    
            except KeyboardInterrupt:
                if Confirm.ask("\nExit?"):
                    break
            except Exception as e:
                logger.error(f"Error: {e}")
                self.console.print(f"[red]Error: {e}[/red]")
    
    def display_welcome(self):
        welcome = """
        [bold cyan]╔═══════════════════════════════════════════════════╗
        ║   AI Data Engineering Pipeline                    ║
        ║   Self-Healing & Natural Language Interface       ║
        ╚═══════════════════════════════════════════════════╝[/bold cyan]
        
        [green]✓ Automatic data cleaning
        ✓ Anomaly detection and auto-fixing
        ✓ Natural language queries
        ✓ Advanced analytics[/green]
        """
        self.console.print(Panel(welcome, border_style="cyan"))
    
    def display_menu(self):
        menu = Table(title="\n[bold cyan]Main Menu[/bold cyan]", show_header=False, border_style="blue")
        menu.add_column("Option", style="cyan", width=5)
        menu.add_column("Description", style="white")
        
        menu.add_row("1", "📁 Upload CSV File")
        menu.add_row("2", "💬 Query Data (Natural Language)")
        menu.add_row("3", "📊 View Statistics")
        menu.add_row("4", "📈 Create Visualizations")
        menu.add_row("5", "✅ View Data Quality")
        menu.add_row("0", "🚪 Exit")
        
        self.console.print(menu)
        
        if self.current_table:
            self.console.print(f"\n[green]Current Dataset:[/green] {self.current_table}")
    
    def upload_csv(self):
        self.console.print("\n[bold]📁 CSV Upload[/bold]\n")
        filepath = Prompt.ask("Enter CSV file path")
        
        if not Path(filepath).exists():
            self.console.print("[red]File not found![/red]")
            return
        
        logger.section("Loading CSV Data")
        df = self.fallback_manager.get_data(mode='csv', filepath=filepath)
        
        if df is None:
            self.console.print("[red]Failed to load CSV![/red]")
            return
        
        self._process_data(df, Path(filepath).stem)
    
    def _process_data(self, df: pd.DataFrame, table_name: str):
        logger.section("Validating Data")
        validation = self.validator.validate(df)
        self.console.print(f"\n[cyan]Quality Score:[/cyan] {validation['quality_score']:.1f}/100")
        
        logger.section("Cleaning Data")
        df_cleaned, _ = self.cleaner.clean_data(df, auto_fix=True)
        
        logger.section("Detecting Anomalies")
        df_flagged, _ = self.anomaly_detector.detect_all(df_cleaned)
        
        if df_flagged['_anomaly_flag'].sum() > 0:
            if Confirm.ask("\nAnomalies detected. Auto-fix?", default=True):
                logger.section("Auto-Fixing Anomalies")
                df_final = self.anomaly_fixer.fix_anomalies(df_flagged, df_flagged['_anomaly_flag'].astype(bool))
            else:
                df_final = df_flagged
        else:
            df_final = df_cleaned
            self.console.print("[green]✓ No anomalies![/green]")
        
        df_final = df_final[[c for c in df_final.columns if not c.startswith('_')]]
        
        logger.section("Storing in Database")
        self.db.create_table_from_dataframe(df_final, table_name)
        self.db.insert_dataframe(df_final, table_name, source=self.data_mode)
        
        self.current_df = df_final
        self.current_table = table_name
        
        self.console.print(f"\n[bold green]✓ Complete![/bold green] Loaded {len(df_final)} rows, {len(df_final.columns)} columns")
    
    def query_data(self):
        if self.current_df is None:
            self.console.print("[yellow]No data loaded. Upload CSV first.[/yellow]")
            return
        
        self.console.print("\n[bold]💬 Natural Language Query[/bold]")
        self.console.print("[dim]Example: 'show me total sales by region'[/dim]\n")
        
        query = Prompt.ask("Enter query")
        
        corrector = GrammarCorrector()
        corrected_query, corrections = corrector.correct(query)
        
        if corrections:
            self.console.print(f"[yellow]Corrected:[/yellow] {corrected_query}")
        
        parser = QueryParser(self.current_df.columns.tolist())
        parsed = parser.parse(corrected_query)
        
        executor = QueryExecutor(self.current_df)
        result = executor.execute(parsed)
        
        self._display_dataframe(result, limit=20)
        self.db.log_query(query, corrected_query if corrections else None, result_count=len(result))
    
    def view_statistics(self):
        if self.current_df is None:
            self.console.print("[yellow]No data loaded.[/yellow]")
            return
        
        self.console.print("\n[bold]📊 Statistics[/bold]\n")
        summary = self.stats_analyzer.get_summary(self.current_df)
        
        self.console.print(f"[cyan]Shape:[/cyan] {summary['shape'][0]} rows × {summary['shape'][1]} columns")
        self.console.print(f"[cyan]Missing:[/cyan] {sum(summary['missing_values'].values())} total\n")
        
        if summary['numeric_summary']:
            self.console.print("[bold]Numeric Summary:[/bold]")
            numeric_df = pd.DataFrame(summary['numeric_summary'])
            self._display_dataframe(numeric_df.round(2))
    
    def create_visualizations(self):
        if self.current_df is None:
            self.console.print("[yellow]No data loaded.[/yellow]")
            return
        
        self.console.print("\n[bold]📈 Visualizations[/bold]\n")
        self.console.print("1. Distribution plot\n2. Correlation heatmap")
        
        choice = Prompt.ask("Select", choices=["1", "2"])
        
        if choice == "1":
            column = Prompt.ask("Column name")
            if column in self.current_df.columns:
                filepath = self.visualizer.plot_distribution(self.current_df, column)
                self.console.print(f"[green]✓ Saved: {filepath}[/green]")
        
        elif choice == "2":
            filepath = self.visualizer.plot_correlation_heatmap(self.current_df)
            if filepath:
                self.console.print(f"[green]✓ Saved: {filepath}[/green]")
    
    def view_data_quality(self):
        if self.current_df is None:
            self.console.print("[yellow]No data loaded.[/yellow]")
            return
        
        self.console.print("\n[bold]✅ Data Quality[/bold]\n")
        validation = self.validator.validate(self.current_df)
        
        self.console.print(f"[cyan]Quality Score:[/cyan] {validation['quality_score']:.1f}/100")
        self.console.print(f"[cyan]Status:[/cyan] {'PASSED' if validation['is_valid'] else 'FAILED'}")
        
        if validation['warnings']:
            self.console.print(f"\n[yellow]Warnings ({len(validation['warnings'])})[/yellow]:")
            for warning in validation['warnings'][:5]:
                self.console.print(f"  ⚠️  {warning}")
    
    def _display_dataframe(self, df: pd.DataFrame, limit: int = 10):
        if df.empty:
            self.console.print("[yellow]No data[/yellow]")
            return
        
        table = Table(show_header=True, header_style="bold cyan")
        for col in df.columns:
            table.add_column(str(col))
        
        for _, row in df.head(limit).iterrows():
            table.add_row(*[str(val) for val in row])
        
        self.console.print(table)
        
        if len(df) > limit:
            self.console.print(f"\n[dim]... and {len(df) - limit} more rows[/dim]")


def main():
    pipeline = DataPipeline()
    pipeline.run()


if __name__ == "__main__":
    main()
