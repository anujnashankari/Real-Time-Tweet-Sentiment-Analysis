#!/usr/bin/env python3
import os
import sys
import time
import signal
import subprocess
import threading
import datetime
from colorama import init, Fore, Style

# Initialize colorama for colored terminal output
init()

# Global variables
processes = {}
stop_event = threading.Event()
LOG_DIR = "logs"

# Configuration
NUM_PRODUCERS = 2  # Number of tweet producer instances to run
NUM_CONSUMERS = 2  # Number of consumer instances to run

def print_colored(text, color=Fore.WHITE, is_bold=False):
    """Print colored text to terminal"""
    style = Style.BRIGHT if is_bold else ""
    timestamp = datetime.datetime.now().strftime("%H:%M:%S")
    print(f"{style}{color}[{timestamp}] {text}{Style.RESET_ALL}")

def run_command(command, name, output_file=None, wait=False):
    """Run a command and capture its output"""
    print_colored(f"Starting {name}...", Fore.BLUE, True)
    
    # Create logs directory if it doesn't exist
    if output_file and not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    
    full_output_path = os.path.join(LOG_DIR, output_file) if output_file else None
    
    if full_output_path:
        with open(full_output_path, 'w') as out_file:
            process = subprocess.Popen(
                command, 
                shell=True,
                stdout=out_file,
                stderr=out_file
            )
    else:
        process = subprocess.Popen(
            command, 
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
    
    processes[name] = process
    
    if wait:
        process.wait()
        return process.returncode
    
    return process

def activate_venv():
    """Activate the virtual environment"""
    venv_path = os.path.expanduser("~/spark-venv")
    activate_script = f"{venv_path}/bin/activate"
    
    if not os.path.exists(activate_script):
        print_colored(f"Creating virtual environment at {venv_path}...", Fore.YELLOW)
        os.system(f"python3 -m venv {venv_path}")
    
    # We need to modify the current environment, not spawn a new process
    print_colored("Activating virtual environment...", Fore.BLUE)
    
    # Set environment variables directly
    os.environ["VIRTUAL_ENV"] = venv_path
    os.environ["PATH"] = f"{venv_path}/bin:{os.environ['PATH']}"
    
    print_colored("Virtual environment activated!", Fore.GREEN)

def start_kafka():
    """Start Kafka server in KRaft mode"""
    kafka_path = os.path.expanduser("~/kafka")
    
    if not os.path.exists(kafka_path):
        print_colored(f"Kafka directory not found at {kafka_path}", Fore.RED)
        print_colored("Please make sure Kafka is installed correctly", Fore.YELLOW)
        return False
    
    print_colored("Starting Kafka server (KRaft mode)...", Fore.BLUE, True)
    kafka_cmd = f"cd {kafka_path} && bin/kafka-server-start.sh config/kraft/server.properties"
    kafka_process = run_command(kafka_cmd, "kafka", "kafka.log")
    
    # Wait for Kafka to start
    print_colored("Waiting for Kafka to start (15 seconds)...", Fore.YELLOW)
    for i in range(15):
        if stop_event.is_set():
            return False
        sys.stdout.write(f"\r{Fore.YELLOW}Starting Kafka: {i+1}/15 seconds elapsed...{Style.RESET_ALL}")
        sys.stdout.flush()
        time.sleep(1)
    print("\n")
    
    print_colored("Kafka should be running now!", Fore.GREEN)
    return True

def start_tweet_producers():
    """Start multiple tweet producer instances"""
    if stop_event.is_set():
        return False
    
    producers = []
    for i in range(NUM_PRODUCERS):
        producer_name = f"producer_{i+1}"
        print_colored(f"Starting tweet producer {i+1}/{NUM_PRODUCERS}...", Fore.BLUE)
        
        producer_type = "tweet" if i == 0 else "user"
        producer_process = run_command(f"python twitter_producer.py --producer-type {producer_type} --instance-id {i+1}", 
                              producer_name, 
                              f"producer_{i+1}.log")
        producers.append(producer_process)
    
    # Wait a bit for the producers to start sending tweets
    print_colored("Waiting for producers to start sending tweets (5 seconds)...", Fore.YELLOW)
    time.sleep(5)
    
    print_colored(f"All {NUM_PRODUCERS} tweet producers running!", Fore.GREEN)
    return True

def start_consumers():
    """Start multiple consumer instances"""
    if stop_event.is_set():
        return False
    
    consumers = []
    
    # First consumer is always the Spark streaming job
    print_colored("Starting Spark Streaming consumer...", Fore.BLUE, True)
    spark_cmd = "spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,mysql:mysql-connector-java:8.0.28 spark_streaming_sentiment.py"
    spark_process = run_command(spark_cmd, "spark_streaming", "spark_streaming.log")
    consumers.append(spark_process)
    
    # Additional consumers (user_data_consumer and others)
    # Initialize consumer processes
    consumers = []
    for i in range(NUM_CONSUMERS):
        consumer_name = f"consumer_{i+1}"
        print_colored(f"Starting consumer {i+1}/{NUM_CONSUMERS}...", Fore.BLUE)

        # Define consumer type and command
        if i == 0:
            # Spark consumer (streaming processor)
            consumer_cmd = "python spark_streaming.py"
        else:
            # User data consumer (or any custom consumer you already have)
            consumer_cmd = f"python user_data_consumer.py --consumer-type user --instance-id {i+1}"
        
        # Run consumer and save process
        consumer_process = run_command(consumer_cmd, consumer_name, f"{consumer_name}.log")
        consumers.append(consumer_process)
    
    # Wait for consumers to initialize
    print_colored("Waiting for consumers to initialize (15 seconds)...", Fore.YELLOW)
    for i in range(15):
        if stop_event.is_set():
            return False
        sys.stdout.write(f"\r{Fore.YELLOW}Initializing consumers: {i+1}/15 seconds elapsed...{Style.RESET_ALL}")
        sys.stdout.flush()
        time.sleep(1)
    print("\n")
    
    print_colored("All consumers should be processing data now!", Fore.GREEN)
    return True

def start_spark_streaming():
    """Start Spark Streaming job (included for backward compatibility)"""
    if stop_event.is_set():
        return False
        
    print_colored("Starting Spark Streaming job...", Fore.BLUE, True)
    spark_cmd = "spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,mysql:mysql-connector-java:8.0.28 spark_streaming_sentiment.py"
    spark_process = run_command(spark_cmd, "spark_streaming", "spark_streaming.log")
    
    # Wait for Spark job to initialize
    print_colored("Waiting for Spark Streaming job to initialize (15 seconds)...", Fore.YELLOW)
    for i in range(15):
        if stop_event.is_set():
            return False
        sys.stdout.write(f"\r{Fore.YELLOW}Initializing Spark: {i+1}/15 seconds elapsed...{Style.RESET_ALL}")
        sys.stdout.flush()
        time.sleep(1)
    print("\n")
    
    print_colored("Spark Streaming should be processing tweets now!", Fore.GREEN)
    return True

def run_batch_analysis():
    """Run batch sentiment analysis"""
    if stop_event.is_set():
        return False
        
    print_colored("\n" + "="*80, Fore.CYAN)
    print_colored("RUNNING BATCH ANALYSIS", Fore.CYAN, True)
    print_colored("="*80 + "\n", Fore.CYAN)
    
    mysql_connector_path = "/mnt/c/Users/anujn/OneDrive/Documents/Sem 6/DBT/mp/mysql-connector-j-8.0.33.jar"
    
    if not os.path.exists(mysql_connector_path):
        print_colored(f"MySQL connector not found at: {mysql_connector_path}", Fore.RED)
        print_colored("Please update the path to your MySQL connector JAR file", Fore.YELLOW)
        return False
    
    batch_cmd = f"spark-submit --jars \"{mysql_connector_path}\" twitter_batch.py"
    print_colored("Executing batch analysis...", Fore.BLUE)
    
    # Run batch analysis and capture output
    batch_process = subprocess.run(batch_cmd, shell=True, capture_output=True, text=True)
    
    # Print results
    if batch_process.stdout:
        print(batch_process.stdout)
    
    if batch_process.stderr and "ERROR" in batch_process.stderr:
        print_colored("Errors from batch analysis:", Fore.RED)
        print(batch_process.stderr)
    
    print_colored("Batch analysis completed!", Fore.GREEN)
    return True

def run_evaluation():
    """Run streaming evaluation"""
    if stop_event.is_set():
        return False
        
    print_colored("\n" + "="*80, Fore.CYAN)
    print_colored("RUNNING STREAMING EVALUATION", Fore.CYAN, True)
    print_colored("="*80 + "\n", Fore.CYAN)
    
    print_colored("Executing streaming evaluation...", Fore.BLUE)
    eval_cmd = "python evaluate_streaming.py"
    
    # Run evaluation and capture output
    eval_process = subprocess.run(eval_cmd, shell=True, capture_output=True, text=True)
    
    # Print results
    if eval_process.stdout:
        print(eval_process.stdout)
    
    if eval_process.stderr:
        print_colored("Errors from evaluation:", Fore.RED)
        print(eval_process.stderr)
    
    print_colored("Streaming evaluation completed!", Fore.GREEN)
    return True

def run_comparison():
    """Run comparison script"""
    if stop_event.is_set():
        return False
        
    print_colored("\n" + "="*80, Fore.CYAN)
    print_colored("COMPARING RESULTS", Fore.CYAN, True)
    print_colored("="*80 + "\n", Fore.CYAN)
    
    print_colored("Executing comparison script...", Fore.BLUE)
    compare_cmd = "python compare_results.py"
    
    # Run comparison and capture output
    compare_process = subprocess.run(compare_cmd, shell=True, capture_output=True, text=True)
    
    # Print results
    if compare_process.stdout:
        print(compare_process.stdout)
    
    if compare_process.stderr:
        print_colored("Errors from comparison:", Fore.RED)
        print(compare_process.stderr)
    
    print_colored("Results comparison completed!", Fore.GREEN)
    return True

def cleanup():
    """Clean up processes"""
    print_colored("\nCleaning up processes...", Fore.YELLOW)
    
    # Set stop event
    stop_event.set()
    
    # Kill each process in reverse order of creation
    for name in list(processes.keys())[::-1]:
        process = processes[name]
        if process and process.poll() is None:  # Check if process exists and is running
            print_colored(f"Stopping {name}...", Fore.YELLOW)
            try:
                process.terminate()
                process.wait(timeout=5)
            except:
                process.kill()
    
    print_colored("All processes stopped! Log files available in ./logs directory", Fore.GREEN)

def signal_handler(sig, frame):
    """Handle keyboard interrupt"""
    print_colored("\n\nInterrupted! Shutting down...", Fore.YELLOW)
    cleanup()
    sys.exit(0)

def main():
    """Main function to orchestrate the entire pipeline"""
    # Create logs directory
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    
    # Register signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    
    # Print header
    print_colored("="*80, Fore.CYAN)
    print_colored(f"TWITTER SENTIMENT ANALYSIS PIPELINE ({NUM_PRODUCERS} PRODUCERS, {NUM_CONSUMERS} CONSUMERS)", Fore.CYAN, True)
    print_colored("="*80, Fore.CYAN)
    
    try:
        # Step 1: Activate virtual environment
        activate_venv()
        
        # Step 2: Start Kafka
        if not start_kafka():
            print_colored("Failed to start Kafka. Exiting...", Fore.RED)
            return
        
        # Step 3: Start multiple producers
        if not start_tweet_producers():
            print_colored("Failed to start producers. Exiting...", Fore.RED)
            cleanup()
            return
        
        # Step 4: Start consumers including Spark Streaming
        if not start_consumers():
            print_colored("Failed to start consumers. Exiting...", Fore.RED)
            cleanup()
            return
        
        # Step 5: Wait for data to accumulate
        wait_time = 60  # seconds
        print_colored(f"\nWaiting {wait_time} seconds for data to accumulate...", Fore.YELLOW)
        for i in range(wait_time):
            if stop_event.is_set():
                break
            progress = int((i / wait_time) * 40)
            sys.stdout.write('\r')
            sys.stdout.write(f"{Fore.YELLOW}[{'=' * progress}{' ' * (40-progress)}] {int((i/wait_time)*100)}%{Style.RESET_ALL}")
            sys.stdout.flush()
            time.sleep(1)
        print("\n")
        
        # Step 6: Run batch analysis
        if not run_batch_analysis():
            print_colored("Batch analysis failed or was skipped.", Fore.RED)
        
        # Step 7: Run evaluation
        if not run_evaluation():
            print_colored("Streaming evaluation failed or was skipped.", Fore.RED)
        
        # Step 8: Run comparison (if needed)
        try:
            if os.path.exists("compare_results.py"):
                if not run_comparison():
                    print_colored("Results comparison failed or was skipped.", Fore.RED)
        except Exception as e:
            print_colored(f"Note: compare_results.py not found or failed: {e}", Fore.YELLOW)
        
        # Step 9: Keep running until interrupted
        print_colored("\nPipeline fully executed! Keeping processes running...", Fore.GREEN, True)
        print_colored("Press Ctrl+C to stop all processes and exit", Fore.YELLOW)
        
        while not stop_event.is_set():
            time.sleep(1)
            
    except Exception as e:
        print_colored(f"Error: {e}", Fore.RED)
    finally:
        cleanup()

if __name__ == "__main__":
    main()