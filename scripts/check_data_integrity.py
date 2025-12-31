#!/usr/bin/env python3
"""
Check IntelliChat data integrity
Run this to diagnose data-related issues
"""

import sqlite3
import sys
from pathlib import Path
from datetime import datetime

# Colors for terminal output
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    RESET = '\033[0m'

PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
CONVERSATIONS_DIR = DATA_DIR / "conversations"
LOGS_DIR = DATA_DIR / "logs"
DB_PATH = CONVERSATIONS_DIR / "conversations.db"

def print_header(text):
    """Print section header"""
    print(f"\n{Colors.BLUE}{'='*60}")
    print(f"{text}")
    print(f"{'='*60}{Colors.RESET}\n")

def print_success(text):
    """Print success message"""
    print(f"{Colors.GREEN}✓ {text}{Colors.RESET}")

def print_warning(text):
    """Print warning message"""
    print(f"{Colors.YELLOW}⚠ {text}{Colors.RESET}")

def print_error(text):
    """Print error message"""
    print(f"{Colors.RED}✗ {text}{Colors.RESET}")

def check_directories():
    """Check if required directories exist"""
    print_header("Checking Directories")
    
    directories = {
        "Data directory": DATA_DIR,
        "Conversations directory": CONVERSATIONS_DIR,
        "Logs directory": LOGS_DIR,
    }
    
    all_exist = True
    for name, path in directories.items():
        if path.exists():
            print_success(f"{name} exists: {path}")
        else:
            print_error(f"{name} missing: {path}")
            all_exist = False
    
    return all_exist

def check_database():
    """Check database file and structure"""
    print_header("Checking Database")
    
    if not DB_PATH.exists():
        print_error(f"Database file not found: {DB_PATH}")
        print_warning("Run: python scripts/initialize_data.py")
        return False
    
    print_success(f"Database file exists: {DB_PATH}")
    
    # Check file size
    size_bytes = DB_PATH.stat().st_size
    size_mb = size_bytes / (1024 * 1024)
    print(f"  Size: {size_mb:.2f} MB ({size_bytes:,} bytes)")
    
    # Check if database can be opened
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Check tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        required_tables = ['conversations', 'messages', 'model_usage']
        
        print("\n  Tables:")
        for table in required_tables:
            if table in tables:
                print_success(f"  {table}")
                
                # Count rows
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"    Rows: {count}")
            else:
                print_error(f"  {table} (missing)")
        
        # Check indexes
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
        indexes = [row[0] for row in cursor.fetchall()]
        
        print(f"\n  Indexes: {len(indexes)}")
        for idx in indexes:
            if not idx.startswith('sqlite_'):  # Skip auto-created indexes
                print(f"    - {idx}")
        
        conn.close()
        print_success("Database structure is valid")
        return True
        
    except sqlite3.Error as e:
        print_error(f"Database error: {e}")
        return False

def check_database_integrity():
    """Run SQLite integrity check"""
    print_header("Database Integrity Check")
    
    if not DB_PATH.exists():
        print_warning("Database doesn't exist, skipping integrity check")
        return False
    
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("PRAGMA integrity_check")
        result = cursor.fetchone()[0]
        
        if result == "ok":
            print_success("Database integrity: OK")
            conn.close()
            return True
        else:
            print_error(f"Database integrity issues: {result}")
            conn.close()
            return False
            
    except sqlite3.Error as e:
        print_error(f"Integrity check failed: {e}")
        return False

def check_logs():
    """Check log files"""
    print_header("Checking Log Files")
    
    log_files = ["app.log", "error.log", "api.log"]
    
    for log_file in log_files:
        log_path = LOGS_DIR / log_file
        if log_path.exists():
            size = log_path.stat().st_size
            size_mb = size / (1024 * 1024)
            
            if size_mb > 100:
                print_warning(f"{log_file} is large: {size_mb:.2f} MB")
                print(f"  Consider archiving: python scripts/archive_logs.py")
            elif size_mb > 10:
                print_success(f"{log_file} exists ({size_mb:.2f} MB)")
            else:
                print_success(f"{log_file} exists ({size:.0f} KB)")
        else:
            print_warning(f"{log_file} not found (will be created on first use)")

def get_statistics():
    """Get database statistics"""
    print_header("Database Statistics")
    
    if not DB_PATH.exists():
        print_warning("Database doesn't exist")
        return
    
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Total conversations
        cursor.execute("SELECT COUNT(*) FROM conversations")
        total_conversations = cursor.fetchone()[0]
        print(f"Total Conversations: {total_conversations}")
        
        # Total messages
        cursor.execute("SELECT COUNT(*) FROM messages")
        total_messages = cursor.fetchone()[0]
        print(f"Total Messages: {total_messages}")
        
        # Messages by role
        cursor.execute("""
            SELECT role, COUNT(*) 
            FROM messages 
            GROUP BY role
        """)
        print("\nMessages by Role:")
        for role, count in cursor.fetchall():
            print(f"  {role}: {count}")
        
        # Total tokens
        cursor.execute("SELECT SUM(token_count) FROM messages WHERE token_count IS NOT NULL")
        total_tokens = cursor.fetchone()[0] or 0
        print(f"\nTotal Tokens Used: {total_tokens:,}")
        
        # Model usage
        cursor.execute("""
            SELECT model_name, COUNT(*) as usage_count
            FROM conversations
            GROUP BY model_name
            ORDER BY usage_count DESC
        """)
        print("\nModel Usage:")
        for model, count in cursor.fetchall():
            print(f"  {model}: {count} conversations")
        
        # Date range
        cursor.execute("SELECT MIN(created_at), MAX(created_at) FROM conversations")
        min_date, max_date = cursor.fetchone()
        if min_date and max_date:
            print(f"\nFirst Conversation: {min_date}")
            print(f"Last Conversation: {max_date}")
        
        conn.close()
        
    except sqlite3.Error as e:
        print_error(f"Error getting statistics: {e}")

def suggest_maintenance():
    """Suggest maintenance actions"""
    print_header("Maintenance Suggestions")
    
    suggestions = []
    
    # Check database size
    if DB_PATH.exists():
        size_mb = DB_PATH.stat().st_size / (1024 * 1024)
        if size_mb > 100:
            suggestions.append(
                "Database is large (>100MB). Consider:\n"
                "  - Vacuum: sqlite3 data/conversations/conversations.db 'VACUUM;'\n"
                "  - Archive old conversations"
            )
    
    # Check log sizes
    for log_file in LOGS_DIR.glob("*.log"):
        size_mb = log_file.stat().st_size / (1024 * 1024)
        if size_mb > 50:
            suggestions.append(
                f"{log_file.name} is large (>50MB). Consider:\n"
                "  - Archive: python scripts/archive_logs.py\n"
                "  - Rotate logs"
            )
    
    if suggestions:
        for i, suggestion in enumerate(suggestions, 1):
            print(f"\n{i}. {suggestion}")
    else:
        print_success("No maintenance needed at this time")

def main():
    """Main function"""
    print(f"\n{Colors.BLUE}{'='*60}")
    print("IntelliChat Data Integrity Check")
    print(f"{'='*60}{Colors.RESET}")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Run all checks
    checks = [
        check_directories(),
        check_database(),
        check_database_integrity(),
    ]
    
    check_logs()
    get_statistics()
    suggest_maintenance()
    
    # Summary
    print_header("Summary")
    
    if all(checks):
        print_success("All checks passed!")
        print("\nYour IntelliChat data is healthy and ready to use.")
        return 0
    else:
        print_warning("Some checks failed")
        print("\nRecommended actions:")
        print("1. Review the errors above")
        print("2. Run: python scripts/initialize_data.py")
        print("3. Check documentation: data/README.md")
        return 1

if __name__ == "__main__":
    sys.exit(main())