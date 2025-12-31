"""
Database connection and initialization
Automatically creates database and tables if they don't exist
"""

import sqlite3
from pathlib import Path
from contextlib import contextmanager
import logging

# Setup logging
logger = logging.getLogger(__name__)

# Database paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
CONVERSATIONS_DIR = DATA_DIR / "conversations"
DB_PATH = CONVERSATIONS_DIR / "conversations.db"

class DatabaseManager:
    """Manages database connections and initialization"""
    
    def __init__(self, db_path=None):
        """Initialize database manager"""
        self.db_path = db_path or DB_PATH
        self._ensure_directories()
        self._initialize_database()
    
    def _ensure_directories(self):
        """Create data directories if they don't exist"""
        DATA_DIR.mkdir(exist_ok=True)
        CONVERSATIONS_DIR.mkdir(exist_ok=True)
        (DATA_DIR / "logs").mkdir(exist_ok=True)
        
        logger.info(f"Data directories ensured at {DATA_DIR}")
    
    def _initialize_database(self):
        """Initialize database with required tables"""
        if not self.db_path.exists():
            logger.info(f"Creating new database at {self.db_path}")
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Create conversations table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS conversations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL UNIQUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    title TEXT,
                    model_name TEXT NOT NULL
                )
            """)
            
            # Create messages table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    conversation_id INTEGER NOT NULL,
                    role TEXT NOT NULL CHECK(role IN ('user', 'assistant', 'system')),
                    content TEXT NOT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    token_count INTEGER,
                    FOREIGN KEY (conversation_id) 
                        REFERENCES conversations(id) 
                        ON DELETE CASCADE
                )
            """)
            
            # Create model_usage table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS model_usage (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    model_name TEXT NOT NULL,
                    tokens_used INTEGER NOT NULL,
                    response_time REAL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes for better performance
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_messages_conversation 
                ON messages(conversation_id)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_conversations_session 
                ON conversations(session_id)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_messages_timestamp 
                ON messages(timestamp DESC)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_model_usage_timestamp 
                ON model_usage(timestamp DESC)
            """)
            
            # Create trigger to update updated_at on conversations
            cursor.execute("""
                CREATE TRIGGER IF NOT EXISTS update_conversation_timestamp 
                AFTER INSERT ON messages
                BEGIN
                    UPDATE conversations 
                    SET updated_at = CURRENT_TIMESTAMP
                    WHERE id = NEW.conversation_id;
                END
            """)
            
            conn.commit()
            logger.info("Database initialized successfully")
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = sqlite3.connect(
            self.db_path,
            check_same_thread=False,
            timeout=10.0
        )
        conn.row_factory = sqlite3.Row  # Enable column access by name
        
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            conn.close()
    
    def execute_query(self, query, params=None):
        """Execute a query and return results"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchall()
    
    def execute_many(self, query, params_list):
        """Execute query with multiple parameter sets"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany(query, params_list)
            conn.commit()
    
    def get_table_info(self, table_name):
        """Get information about a table"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            return cursor.fetchall()
    
    def vacuum(self):
        """Optimize database (compress and defragment)"""
        logger.info("Running VACUUM on database")
        with self.get_connection() as conn:
            conn.execute("VACUUM")
        logger.info("VACUUM completed")
    
    def check_integrity(self):
        """Check database integrity"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA integrity_check")
            result = cursor.fetchone()[0]
            return result == "ok"
    
    def get_statistics(self):
        """Get database statistics"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            stats = {}
            
            # Total conversations
            cursor.execute("SELECT COUNT(*) FROM conversations")
            stats['total_conversations'] = cursor.fetchone()[0]
            
            # Total messages
            cursor.execute("SELECT COUNT(*) FROM messages")
            stats['total_messages'] = cursor.fetchone()[0]
            
            # Total tokens
            cursor.execute("SELECT COALESCE(SUM(token_count), 0) FROM messages")
            stats['total_tokens'] = cursor.fetchone()[0]
            
            # Database size
            cursor.execute("SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()")
            stats['db_size_bytes'] = cursor.fetchone()[0]
            stats['db_size_mb'] = stats['db_size_bytes'] / (1024 * 1024)
            
            return stats

# Global database manager instance
db_manager = DatabaseManager()

def get_db():
    """Get database manager instance"""
    return db_manager

# Convenience functions
def get_connection():
    """Get database connection (context manager)"""
    return db_manager.get_connection()

def execute_query(query, params=None):
    """Execute a query"""
    return db_manager.execute_query(query, params)

def initialize_database():
    """Initialize database (called automatically)"""
    db_manager._initialize_database()
    return True

if __name__ == "__main__":
    # Test database initialization
    print("Testing database initialization...")
    
    # Create database
    initialize_database()
    
    # Get statistics
    stats = db_manager.get_statistics()
    print("\nDatabase Statistics:")
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    # Check integrity
    if db_manager.check_integrity():
        print("\n✓ Database integrity check passed")
    else:
        print("\n✗ Database integrity check failed")
    
    print("\n✓ Database initialization successful!")