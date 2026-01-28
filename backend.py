import sqlite3
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import pandas as pd
import hashlib
import os

class AttendanceDatabase:
    def __init__(self, db_name="attendance_system.db"):
        self.db_name = db_name
        self.init_database()
    
    def init_database(self):
        """Initialize database with required tables"""
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        
        # Professors table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS professors (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                department TEXT NOT NULL,
                contact TEXT,
                email TEXT UNIQUE,
                date_registered DATE NOT NULL,
                is_active BOOLEAN DEFAULT 1,
                password_hash TEXT,
                role TEXT DEFAULT 'professor'
            )
        ''')
        
        # Attendance records table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS attendance_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                professor_id TEXT NOT NULL,
                session_id INTEGER,
                date DATE NOT NULL,
                time_in TIME NOT NULL,
                time_out TIME,
                status TEXT DEFAULT 'Present',
                venue TEXT,
                remarks TEXT,
                session_type TEXT,
                latitude REAL,
                longitude REAL,
                device_id TEXT,
                FOREIGN KEY (professor_id) REFERENCES professors(id),
                FOREIGN KEY (session_id) REFERENCES attendance_sessions(id)
            )
        ''')
        
        # Attendance sessions table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS attendance_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_type TEXT NOT NULL,
                venue TEXT NOT NULL,
                remarks TEXT,
                date DATE NOT NULL,
                start_time TIME NOT NULL,
                end_time TIME,
                created_by TEXT,
                qr_code_data TEXT,
                is_active BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Courses/Subjects table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS courses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                course_code TEXT NOT NULL UNIQUE,
                course_name TEXT NOT NULL,
                department TEXT,
                units INTEGER,
                semester TEXT,
                academic_year TEXT
            )
        ''')
        
        # Professor courses assignment
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS professor_courses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                professor_id TEXT NOT NULL,
                course_id INTEGER NOT NULL,
                schedule TEXT,
                room TEXT,
                FOREIGN KEY (professor_id) REFERENCES professors(id),
                FOREIGN KEY (course_id) REFERENCES courses(id)
            )
        ''')
        
        # Admin users table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS admins (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                full_name TEXT NOT NULL,
                role TEXT DEFAULT 'admin',
                permissions TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # System logs
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS system_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                user_id TEXT,
                action TEXT NOT NULL,
                details TEXT,
                ip_address TEXT,
                device_info TEXT
            )
        ''')
        
        conn.commit()
        conn.close()
    
    # Professor Management
    def add_professor(self, professor_data: Dict) -> Tuple[bool, str]:
        """Add a new professor to the database"""
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()
            
            # Check if professor already exists
            cursor.execute("SELECT id FROM professors WHERE id = ? OR email = ?", 
                         (professor_data['id'], professor_data.get('email', '')))
            if cursor.fetchone():
                return False, "Professor ID or email already exists"
            
            cursor.execute('''
                INSERT INTO professors (id, name, department, contact, email, date_registered)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                professor_data['id'],
                professor_data['name'],
                professor_data['department'],
                professor_data.get('contact', ''),
                professor_data.get('email', ''),
                datetime.now().strftime('%Y-%m-%d')
            ))
            
            conn.commit()
            conn.close()
            
            self.log_action("ADD_PROFESSOR", f"Added professor {professor_data['id']}")
            return True, "Professor added successfully"
            
        except Exception as e:
            return False, f"Error adding professor: {str(e)}"
    
    def get_professor(self, professor_id: str) -> Optional[Dict]:
        """Retrieve professor details"""
        conn = sqlite3.connect(self.db_name)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM professors WHERE id = ?", (professor_id,))
        row = cursor.fetchone()
        
        conn.close()
        return dict(row) if row else None
    
    def get_all_professors(self, department_filter: str = None) -> List[Dict]:
        """Get all professors, optionally filtered by department"""
        conn = sqlite3.connect(self.db_name)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        if department_filter:
            cursor.execute("SELECT * FROM professors WHERE department = ? AND is_active = 1 ORDER BY name", 
                         (department_filter,))
        else:
            cursor.execute("SELECT * FROM professors WHERE is_active = 1 ORDER BY name")
        
        professors = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return professors
    
    # Attendance Management
    def record_attendance(self, attendance_data: Dict) -> Tuple[bool, str]:
        """Record professor attendance"""
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()
            
            # Check if attendance already recorded for this session
            cursor.execute('''
                SELECT id FROM attendance_records 
                WHERE professor_id = ? AND session_id = ? AND date = ?
            ''', (
                attendance_data['professor_id'],
                attendance_data.get('session_id'),
                attendance_data['date']
            ))
            
            if cursor.fetchone():
                return False, "Attendance already recorded for this session"
            
            cursor.execute('''
                INSERT INTO attendance_records 
                (professor_id, session_id, date, time_in, status, venue, session_type, remarks)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                attendance_data['professor_id'],
                attendance_data.get('session_id'),
                attendance_data['date'],
                attendance_data['time_in'],
                attendance_data.get('status', 'Present'),
                attendance_data.get('venue', ''),
                attendance_data.get('session_type', ''),
                attendance_data.get('remarks', '')
            ))
            
            conn.commit()
            conn.close()
            
            self.log_action("RECORD_ATTENDANCE", 
                          f"Professor {attendance_data['professor_id']} attendance recorded")
            return True, "Attendance recorded successfully"
            
        except Exception as e:
            return False, f"Error recording attendance: {str(e)}"
    
    def get_attendance_records(self, filters: Dict = None) -> List[Dict]:
        """Get attendance records with optional filters"""
        conn = sqlite3.connect(self.db_name)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        query = '''
            SELECT ar.*, p.name, p.department 
            FROM attendance_records ar
            JOIN professors p ON ar.professor_id = p.id
            WHERE 1=1
        '''
        params = []
        
        if filters:
            if filters.get('date'):
                query += " AND ar.date = ?"
                params.append(filters['date'])
            if filters.get('professor_id'):
                query += " AND ar.professor_id = ?"
                params.append(filters['professor_id'])
            if filters.get('session_type'):
                query += " AND ar.session_type = ?"
                params.append(filters['session_type'])
            if filters.get('department'):
                query += " AND p.department = ?"
                params.append(filters['department'])
            if filters.get('start_date') and filters.get('end_date'):
                query += " AND ar.date BETWEEN ? AND ?"
                params.extend([filters['start_date'], filters['end_date']])
        
        query += " ORDER BY ar.date DESC, ar.time_in DESC"
        
        cursor.execute(query, params)
        records = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return records
    
    def get_attendance_summary(self, start_date: str, end_date: str) -> Dict:
        """Get attendance summary for a date range"""
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        
        # Total attendance count
        cursor.execute('''
            SELECT COUNT(*) FROM attendance_records 
            WHERE date BETWEEN ? AND ?
        ''', (start_date, end_date))
        total_attendance = cursor.fetchone()[0]
        
        # Attendance by department
        cursor.execute('''
            SELECT p.department, COUNT(*) as count
            FROM attendance_records ar
            JOIN professors p ON ar.professor_id = p.id
            WHERE ar.date BETWEEN ? AND ?
            GROUP BY p.department
        ''', (start_date, end_date))
        by_department = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Attendance by session type
        cursor.execute('''
            SELECT session_type, COUNT(*) as count
            FROM attendance_records
            WHERE date BETWEEN ? AND ?
            GROUP BY session_type
        ''', (start_date, end_date))
        by_session = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Top attending professors
        cursor.execute('''
            SELECT p.name, COUNT(*) as attendance_count
            FROM attendance_records ar
            JOIN professors p ON ar.professor_id = p.id
            WHERE ar.date BETWEEN ? AND ?
            GROUP BY ar.professor_id
            ORDER BY attendance_count DESC
            LIMIT 10
        ''', (start_date, end_date))
        top_professors = [{"name": row[0], "count": row[1]} for row in cursor.fetchall()]
        
        conn.close()
        
        return {
            "total_attendance": total_attendance,
            "by_department": by_department,
            "by_session_type": by_session,
            "top_professors": top_professors,
            "date_range": f"{start_date} to {end_date}"
        }
    
    # Session Management
    def create_attendance_session(self, session_data: Dict) -> Tuple[bool, str, int]:
        """Create a new attendance session"""
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO attendance_sessions 
                (session_type, venue, remarks, date, start_time, end_time, created_by, qr_code_data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                session_data['session_type'],
                session_data['venue'],
                session_data.get('remarks', ''),
                session_data['date'],
                session_data['start_time'],
                session_data.get('end_time'),
                session_data.get('created_by', 'system'),
                session_data.get('qr_code_data', '')
            ))
            
            session_id = cursor.lastrowid
            conn.commit()
            conn.close()
            
            self.log_action("CREATE_SESSION", f"Created session {session_id}")
            return True, "Session created successfully", session_id
            
        except Exception as e:
            return False, f"Error creating session: {str(e)}", -1
    
    def get_active_sessions(self) -> List[Dict]:
        """Get all active attendance sessions"""
        conn = sqlite3.connect(self.db_name)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM attendance_sessions 
            WHERE is_active = 1 AND date = ?
            ORDER BY start_time
        ''', (datetime.now().strftime('%Y-%m-%d'),))
        
        sessions = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return sessions
    
    # Course Management
    def add_course(self, course_data: Dict) -> Tuple[bool, str]:
        """Add a new course"""
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO courses (course_code, course_name, department, units, semester, academic_year)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                course_data['course_code'],
                course_data['course_name'],
                course_data.get('department', ''),
                course_data.get('units', 3),
                course_data.get('semester', '1st'),
                course_data.get('academic_year', '2024-2025')
            ))
            
            conn.commit()
            conn.close()
            return True, "Course added successfully"
            
        except Exception as e:
            return False, f"Error adding course: {str(e)}"
    
    def assign_course_to_professor(self, professor_id: str, course_id: int, 
                                   schedule: str = None, room: str = None) -> Tuple[bool, str]:
        """Assign a course to a professor"""
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()
            
            # Check if assignment already exists
            cursor.execute('''
                SELECT id FROM professor_courses 
                WHERE professor_id = ? AND course_id = ?
            ''', (professor_id, course_id))
            
            if cursor.fetchone():
                return False, "Course already assigned to this professor"
            
            cursor.execute('''
                INSERT INTO professor_courses (professor_id, course_id, schedule, room)
                VALUES (?, ?, ?, ?)
            ''', (professor_id, course_id, schedule, room))
            
            conn.commit()
            conn.close()
            return True, "Course assigned successfully"
            
        except Exception as e:
            return False, f"Error assigning course: {str(e)}"
    
    def get_professor_schedule(self, professor_id: str) -> List[Dict]:
        """Get professor's course schedule"""
        conn = sqlite3.connect(self.db_name)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT c.course_code, c.course_name, pc.schedule, pc.room, c.department
            FROM professor_courses pc
            JOIN courses c ON pc.course_id = c.id
            WHERE pc.professor_id = ?
            ORDER BY pc.schedule
        ''', (professor_id,))
        
        schedule = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return schedule
    
    # Admin Authentication
    def create_admin(self, username: str, password: str, full_name: str, 
                     role: str = "admin") -> Tuple[bool, str]:
        """Create a new admin user"""
        try:
            password_hash = hashlib.sha256(password.encode()).hexdigest()
            
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO admins (username, password_hash, full_name, role)
                VALUES (?, ?, ?, ?)
            ''', (username, password_hash, full_name, role))
            
            conn.commit()
            conn.close()
            return True, "Admin created successfully"
            
        except sqlite3.IntegrityError:
            return False, "Username already exists"
        except Exception as e:
            return False, f"Error creating admin: {str(e)}"
    
    def authenticate_admin(self, username: str, password: str) -> Tuple[bool, Optional[Dict]]:
        """Authenticate admin user"""
        password_hash = hashlib.sha256(password.encode()).hexdigest()
        
        conn = sqlite3.connect(self.db_name)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM admins WHERE username = ? AND password_hash = ?
        ''', (username, password_hash))
        
        admin = cursor.fetchone()
        conn.close()
        
        if admin:
            self.log_action("ADMIN_LOGIN", f"Admin {username} logged in")
            return True, dict(admin)
        return False, None
    
    # Reporting and Analytics
    def generate_detailed_report(self, start_date: str, end_date: str, 
                                 department: str = None) -> pd.DataFrame:
        """Generate detailed attendance report"""
        conn = sqlite3.connect(self.db_name)
        
        query = '''
            SELECT 
                ar.date,
                ar.time_in,
                ar.time_out,
                p.id as professor_id,
                p.name,
                p.department,
                ar.session_type,
                ar.venue,
                ar.status,
                ar.remarks,
                asess.start_time as session_start,
                asess.end_time as session_end
            FROM attendance_records ar
            JOIN professors p ON ar.professor_id = p.id
            LEFT JOIN attendance_sessions asess ON ar.session_id = asess.id
            WHERE ar.date BETWEEN ? AND ?
        '''
        params = [start_date, end_date]
        
        if department:
            query += " AND p.department = ?"
            params.append(department)
        
        query += " ORDER BY ar.date, ar.time_in"
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        return df
    
    def get_daily_attendance_stats(self, date: str = None) -> Dict:
        """Get daily attendance statistics"""
        if not date:
            date = datetime.now().strftime('%Y-%m-%d')
        
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        
        # Total professors
        cursor.execute("SELECT COUNT(*) FROM professors WHERE is_active = 1")
        total_professors = cursor.fetchone()[0]
        
        # Professors who attended today
        cursor.execute('''
            SELECT COUNT(DISTINCT professor_id) 
            FROM attendance_records 
            WHERE date = ?
        ''', (date,))
        attended_today = cursor.fetchone()[0]
        
        # By department
        cursor.execute('''
            SELECT p.department, COUNT(DISTINCT ar.professor_id) as count
            FROM professors p
            LEFT JOIN attendance_records ar ON p.id = ar.professor_id AND ar.date = ?
            WHERE p.is_active = 1
            GROUP BY p.department
        ''', (date,))
        department_stats = {row[0]: row[1] for row in cursor.fetchall()}
        
        conn.close()
        
        return {
            "date": date,
            "total_professors": total_professors,
            "attended_today": attended_today,
            "attendance_rate": (attended_today / total_professors * 100) if total_professors > 0 else 0,
            "by_department": department_stats,
            "absent_today": total_professors - attended_today
        }
    
    # Utility Methods
    def log_action(self, action: str, details: str, user_id: str = None):
        """Log system actions"""
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO system_logs (user_id, action, details)
                VALUES (?, ?, ?)
            ''', (user_id, action, details))
            
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Error logging action: {e}")
    
    def backup_database(self, backup_path: str) -> bool:
        """Create a backup of the database"""
        try:
            conn = sqlite3.connect(self.db_name)
            backup_conn = sqlite3.connect(backup_path)
            
            conn.backup(backup_conn)
            
            backup_conn.close()
            conn.close()
            
            self.log_action("DATABASE_BACKUP", f"Backup created at {backup_path}")
            return True
        except Exception as e:
            print(f"Error creating backup: {e}")
            return False
    
    def export_to_excel(self, filters: Dict = None, output_path: str = None) -> str:
        """Export attendance data to Excel"""
        if not output_path:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = f"attendance_report_{timestamp}.xlsx"
        
        # Get data
        records = self.get_attendance_records(filters)
        
        if not records:
            return ""
        
        df = pd.DataFrame(records)
        
        # Export to Excel with formatting
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Attendance Records', index=False)
            
            # Auto-adjust column widths
            worksheet = writer.sheets['Attendance Records']
            for column in df:
                column_length = max(df[column].astype(str).map(len).max(), len(column))
                col_idx = df.columns.get_loc(column)
                worksheet.column_dimensions[chr(65 + col_idx)].width = column_length + 2
        
        self.log_action("EXPORT_EXCEL", f"Exported to {output_path}")
        return output_path
    
    def get_dashboard_stats(self) -> Dict:
        """Get dashboard statistics"""
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        
        # Total professors
        cursor.execute("SELECT COUNT(*) FROM professors WHERE is_active = 1")
        total_professors = cursor.fetchone()[0]
        
        # Today's attendance
        today = datetime.now().strftime('%Y-%m-%d')
        cursor.execute('''
            SELECT COUNT(DISTINCT professor_id) 
            FROM attendance_records 
            WHERE date = ?
        ''', (today,))
        today_attendance = cursor.fetchone()[0]
        
        # This month's attendance
        month_start = datetime.now().replace(day=1).strftime('%Y-%m-%d')
        cursor.execute('''
            SELECT COUNT(*) 
            FROM attendance_records 
            WHERE date BETWEEN ? AND ?
        ''', (month_start, today))
        month_attendance = cursor.fetchone()[0]
        
        # Active sessions today
        cursor.execute('''
            SELECT COUNT(*) 
            FROM attendance_sessions 
            WHERE date = ? AND is_active = 1
        ''', (today,))
        active_sessions = cursor.fetchone()[0]
        
        # Recent attendance (last 7 days)
        cursor.execute('''
            SELECT date, COUNT(*) as count
            FROM attendance_records
            WHERE date >= date(?, '-7 days')
            GROUP BY date
            ORDER BY date DESC
        ''', (today,))
        recent_attendance = {row[0]: row[1] for row in cursor.fetchall()}
        
        conn.close()
        
        return {
            "total_professors": total_professors,
            "today_attendance": today_attendance,
            "month_attendance": month_attendance,
            "active_sessions": active_sessions,
            "attendance_rate": (today_attendance / total_professors * 100) if total_professors > 0 else 0,
            "recent_attendance": recent_attendance
        }