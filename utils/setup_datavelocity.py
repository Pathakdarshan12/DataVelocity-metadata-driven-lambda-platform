import os
import time
from datetime import datetime
from typing import List, Dict, Optional
from utils.snowflake_connector import SnowflakeConnection


class SQLScriptExecutor:
    """Robust SQL script executor with logging, error handling, and rollback"""

    def __init__(self):
        self.sf = SnowflakeConnection()
        self.execution_log: List[Dict] = []
        self.failed_scripts: List[str] = []

    def execute_scripts(self, script_definitions: List[Dict],
                        stop_on_error: bool = True,
                        create_savepoint: bool = True):
        """
        Execute multiple SQL scripts with robust error handling

        Args:
            script_definitions: List of dicts with 'file' and optional 'description'
            stop_on_error: Whether to stop execution on first error
            create_savepoint: Create savepoints before each script
        """
        self.sf.connect()
        start_time = datetime.now()

        print("=" * 80)
        print(f"🚀 Starting SQL Script Execution Pipeline")
        print(f"📅 Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📋 Total scripts: {len(script_definitions)}")
        print("=" * 80)
        print()

        try:
            with self.sf.get_cursor() as cursor:
                # Show current context
                cursor.execute("SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()")
                role, db, schema, warehouse = cursor.fetchone()
                print(f"🔐 Context: Role={role}, DB={db}, Schema={schema}, Warehouse={warehouse}\n")

                # Execute each script
                for idx, script_def in enumerate(script_definitions, 1):
                    script_file = script_def['file']
                    description = script_def.get('description', script_file)
                    is_optional = script_def.get('optional', False)

                    success = self._execute_single_script(
                        cursor=cursor,
                        script_file=script_file,
                        description=description,
                        script_number=idx,
                        total_scripts=len(script_definitions),
                        create_savepoint=create_savepoint,
                        is_optional=is_optional
                    )

                    if not success and not is_optional and stop_on_error:
                        print(f"\n❌ Stopping execution due to error in: {script_file}")
                        break

                # Summary
                self._print_summary(start_time)

        except Exception as e:
            print(f"\n💥 Critical error during execution: {e}")
            import traceback
            traceback.print_exc()

        finally:
            self.sf.close()
            self._save_execution_log()

    def _execute_single_script(self, cursor, script_file: str, description: str,
                               script_number: int, total_scripts: int,
                               create_savepoint: bool, is_optional: bool) -> bool:
        """Execute a single SQL script with error handling"""

        print("=" * 80)
        print(f"📄 [{script_number}/{total_scripts}] {description}")
        print(f"📂 File: {script_file}")
        if is_optional:
            print("⚠️  Optional script - errors will not stop execution")
        print("=" * 80)

        start_time = time.time()
        log_entry = {
            'script': script_file,
            'description': description,
            'start_time': datetime.now().isoformat(),
            'status': 'pending'
        }

        try:
            # Construct file path
            base_dir = os.path.dirname(os.path.abspath(__file__))
            file_path = os.path.abspath(os.path.join(base_dir, "..", script_file))

            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Script file not found: {file_path}")

            # Read SQL content
            with open(file_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()

            if not sql_content.strip():
                raise ValueError(f"Script file is empty: {script_file}")

            # Clean SQL content - remove empty statements and comments
            cleaned_sql = self._clean_sql_content(sql_content)

            # Create savepoint if requested
            savepoint_name = None
            if create_savepoint:
                savepoint_name = f"sp_{script_number}_{int(time.time())}"
                try:
                    cursor.execute(f"BEGIN")
                    print(f"💾 Transaction started")
                except Exception as e:
                    print(f"⚠️  Could not create transaction: {e}")

            # Execute entire script as one block using connection.execute_string for multi-statement support
            print(f"📊 Executing entire script...\n")

            try:
                print(f"  ⚙️  Running script...")
                # Use connection.execute_string() which supports multiple statements
                results_generator = self.sf.connection.execute_string(cleaned_sql)

                # Process all results from all statements
                statement_count = 0
                for cur in results_generator:
                    statement_count += 1
                    try:
                        results = cur.fetchall()
                        if results:
                            print(f"  ✅ Statement {statement_count} returned {len(results)} row(s)")
                            if len(results) <= 10:  # Only show small result sets
                                for row in results:
                                    print(f"     {row}")
                    except:
                        pass  # No results to fetch

                print(f"  ✅ Executed {statement_count} statement(s)")

            except Exception as script_error:
                print(f"  ❌ Script execution failed: {script_error}")
                raise

            # Commit transaction
            if create_savepoint:
                cursor.execute("COMMIT")
                print(f"✅ Transaction committed")

            # Success
            execution_time = time.time() - start_time
            print(f"\n✅ Completed successfully in {execution_time:.2f}s")
            print()

            log_entry['status'] = 'success'
            log_entry['execution_time'] = execution_time
            self.execution_log.append(log_entry)

            return True

        except FileNotFoundError as e:
            print(f"\n❌ File Error: {e}\n")
            log_entry['status'] = 'file_not_found'
            log_entry['error'] = str(e)
            self.execution_log.append(log_entry)
            self.failed_scripts.append(script_file)
            return False

        except Exception as e:
            execution_time = time.time() - start_time
            print(f"\n❌ Execution Failed after {execution_time:.2f}s")
            print(f"Error: {e}\n")

            # Rollback if in transaction
            if create_savepoint:
                try:
                    cursor.execute("ROLLBACK")
                    print(f"↩️  Transaction rolled back\n")
                except:
                    pass

            log_entry['status'] = 'failed' if not is_optional else 'optional_failed'
            log_entry['error'] = str(e)
            log_entry['execution_time'] = execution_time
            self.execution_log.append(log_entry)

            if not is_optional:
                self.failed_scripts.append(script_file)

            # Print traceback for debugging
            if not is_optional:
                import traceback
                traceback.print_exc()
                print()

            return False

    def _clean_sql_content(self, sql_content: str) -> str:
        """Clean SQL content by removing comments and empty statements"""
        lines = []
        for line in sql_content.split('\n'):
            # Remove single-line comments
            if '--' in line:
                line = line[:line.index('--')]
            # Keep non-empty lines
            if line.strip():
                lines.append(line)

        return '\n'.join(lines)

    def _print_summary(self, start_time: datetime):
        """Print execution summary"""
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        success_count = sum(1 for log in self.execution_log if log['status'] == 'success')
        failed_count = sum(1 for log in self.execution_log if log['status'] == 'failed')
        optional_failed = sum(1 for log in self.execution_log if log['status'] == 'optional_failed')

        print("\n" + "=" * 80)
        print("📊 EXECUTION SUMMARY")
        print("=" * 80)
        print(f"⏱️  Total Duration: {duration:.2f}s")
        print(f"✅ Successful: {success_count}")
        print(f"❌ Failed: {failed_count}")
        if optional_failed > 0:
            print(f"⚠️  Optional Failed: {optional_failed}")
        print(f"📋 Total: {len(self.execution_log)}")

        if self.failed_scripts:
            print(f"\n❌ Failed Scripts:")
            for script in self.failed_scripts:
                print(f"   - {script}")

        print("=" * 80)

        if failed_count == 0:
            print("🎉 All scripts executed successfully!")
        else:
            print("⚠️  Some scripts failed. Check logs for details.")
        print("=" * 80)

    def _save_execution_log(self):
        """Save execution log to file"""
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = os.path.join(log_dir, f"sql_execution_{timestamp}.log")

        try:
            with open(log_file, 'w') as f:
                f.write("SQL Script Execution Log\n")
                f.write("=" * 80 + "\n\n")

                for log_entry in self.execution_log:
                    f.write(f"Script: {log_entry['script']}\n")
                    f.write(f"Status: {log_entry['status']}\n")
                    f.write(f"Start Time: {log_entry['start_time']}\n")

                    if 'execution_time' in log_entry:
                        f.write(f"Execution Time: {log_entry['execution_time']:.2f}s\n")

                    if 'error' in log_entry:
                        f.write(f"Error: {log_entry['error']}\n")

                    f.write("\n" + "-" * 80 + "\n\n")

            print(f"📝 Execution log saved to: {log_file}")

        except Exception as e:
            print(f"⚠️  Could not save log file: {e}")

def run_datavelocity_pipeline():
    """Run the complete DataVelocity pipeline"""

    # Define all scripts in execution order
    scripts = [
        # 1. Infrastructure Setup
        # {
        #     'file': 'setup/setup_datavelocity_pipeline.sql',
        #     'description': '🏗️  Setup DataVelocity Pipeline Infrastructure'
        # },

        # 2. Deploy Entities (in dependency order)
        {
            'file': 'entities/location.sql',
            'description': '📍 Deploy Location Entity'
        },
        {
            'file': 'entities/customer.sql',
            'description': '👤 Deploy Customer Entity'
        },
        {
            'file': 'entities/customer_address.sql',
            'description': '🏠 Deploy Customer Address Entity'
        },
        {
            'file': 'entities/restaurant.sql',
            'description': '🍽️  Deploy Restaurant Entity'
        },
        {
            'file': 'entities/menu.sql',
            'description': '📋 Deploy Menu Entity'
        },
        {
            'file': 'entities/delivery_agent.sql',
            'description': '🚴 Deploy Delivery Agent Entity'
        },
        {
            'file': 'entities/order.sql',
            'description': '🛒 Deploy Order Entity'
        },
        {
            'file': 'entities/order_item.sql',
            'description': '📦 Deploy Order Item Entity'
        },
        {
            'file': 'entities/delivery.sql',
            'description': '🚚 Deploy Delivery Entity'
        },
        #
        # # 3. Data Quality Configuration
        # {
        #     'file': 'orchestration/dq_config_and_validation.sql',
        #     'description': '✅ Configure Data Quality & Validation'
        # },
        #
        # # 4. Orchestration
        # {
        #     'file': 'orchestration/import_master_procedure.sql',
        #     'description': '⚙️  Deploy Master Import Procedure'
        # },
        #
        # # 5. Analytics
        # {
        #     'file': 'analytics/dim_date.sql',
        #     'description': '📅 Deploy Date Dimension'
        # },
        # {
        #     'file': 'analytics/mart_orders.sql',
        #     'description': '📊 Deploy Orders Mart'
        # },
        #
        # # 6. Streaming (Optional)
        # {
        #     'file': 'streaming/Kafka_Streaming_Setup.sql',
        #     'description': '🌊 Setup Kafka Streaming',
        #     'optional': True
        # },
        # {
        #     'file': 'streaming/Kafka_Tables_Streams_Procedure_Task_Setup.sql',
        #     'description': '⚡ Deploy Kafka Tables, Streams & Tasks',
        #     'optional': True
        # },
    ]

    # Execute all scripts
    executor = SQLScriptExecutor()
    executor.execute_scripts(
        script_definitions=scripts,
        stop_on_error=True,  # Stop on first error
        create_savepoint=True  # Create transactions for rollback
    )


if __name__ == "__main__":
    run_datavelocity_pipeline()