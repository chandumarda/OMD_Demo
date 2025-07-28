"""
Generic File-Based Connector Template
Supports local files, network file systems, cloud storage, and any file-based data source.
"""

import os
import glob
from pathlib import Path
from typing import List, Dict, Tuple, Any, Optional
from urllib.parse import urlparse
import logging

from .base_connector import CommonConnectorSource, ConnectorSecurityManager
from ..parsers.factory import ParserFactory


class ReferencedCSVParser:
    def __init__(self, file_path: str, file_handler=None):
        self.file_path = file_path

    def get_schema(self) -> List[Dict[str, str]]:
        return [
            {"name": "Database", "type": "string"},
            {"name": "Schema", "type": "string"},
            {"name": "Table", "type": "string"}
        ]

    def get_sample_data(self, limit: int = 100) -> List[Dict[str, str]]:
        sample_data = []
        try:
            with open(self.file_path, mode='r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for i, row in enumerate(reader):
                    if i >= limit:
                        break
                    sample_data.append({
                        "Database": row.get("Database", "").strip(),
                        "Schema": row.get("Schema", "").strip(),
                        "Table": row.get("Table", "").strip()
                    })
        except Exception as e:
            print(f"Error reading sample data: {e}")
        return sample_data

    def get_record_count(self) -> int:
        count = 0
        try:
            with open(self.file_path, mode='r', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                next(reader, None)  # Skip header
                for _ in reader:
                    count += 1
        except Exception as e:
            print(f"Error counting records: {e}")
        return count


logger = logging.getLogger(__name__)


class FileBasedConnectorSource(CommonConnectorSource):
    """
    Generic file-based connector that can be adapted for various file systems:
    - Local file systems
    - Network file systems (NFS, SMB/CIFS)
    - FTP/SFTP servers
    - Cloud storage services
    - Distributed file systems (HDFS)
    """

    def __init__(self, config, metadata_config):
        super().__init__(config, metadata_config)
        
        # File system configuration
        self.base_path = self.connection_options.get('basePath', '/')
        self.file_patterns = self.connection_options.get('filePatterns', ['*'])
        self.recursive = self.connection_options.get('recursive', True)
        self.max_file_size = self.connection_options.get('maxFileSizeBytes', 1024 * 1024 * 1024)  # 1GB default
        
        # Supported file formats
        self.supported_formats = self.connection_options.get('supportedFormats', 
                                                           ['csv', 'json', 'jsonl', 'parquet', 'avro', 'excel'])
        
        # Partition detection
        self.enable_partitioning = self.connection_options.get('enablePartitioning', False)
        self.partition_pattern = self.connection_options.get('partitionPattern', r'(\w+)=([^/]+)')
        
        # File system specific settings
        self.file_system_type = self.connection_options.get('fileSystemType', 'local')
        
        # Initialize file system handler
        self.file_handler = self._get_file_handler()
        
        # Security manager
        security_config = self.connection_options.get('securityConfig', {})
        self.security_manager = ConnectorSecurityManager(security_config)

    def _get_file_handler(self):
        """Get the appropriate file system handler based on configuration."""
        if self.file_system_type == 'local':
            return LocalFileHandler(self.connection_options)
        elif self.file_system_type == 'ftp':
            return FTPFileHandler(self.connection_options)
        elif self.file_system_type == 'sftp':
            return SFTPFileHandler(self.connection_options)
        elif self.file_system_type == 'hdfs':
            return HDFSFileHandler(self.connection_options)
        elif self.file_system_type == 'cloud':
            return CloudFileHandler(self.connection_options)
        else:
            raise ValueError(f"Unsupported file system type: {self.file_system_type}")

    def prepare(self):
        """Initialize the file system connection."""
        try:
            # Authenticate if required
            if not self.security_manager.authenticate():
                raise Exception("Authentication failed")
            
            # Initialize file handler
            self.file_handler.connect()
            
            # Validate base path exists
            if not self.file_handler.path_exists(self.base_path):
                raise Exception(f"Base path does not exist: {self.base_path}")
            
            logger.info(f"File-based connector initialized for {self.file_system_type} at {self.base_path}")
            
        except Exception as e:
            logger.error(f"Failed to prepare file-based connector: {str(e)}")
            raise e

    def get_database_names(self) -> List[str]:
        """
        For file-based systems, databases are typically top-level directories.
        """
        try:
            if self.enable_partitioning:
                # If partitioning is enabled, look for partition directories
                return self._get_partition_databases()
            else:
                # Otherwise, use direct subdirectories as databases
                return self._get_directory_databases()
                
        except Exception as e:
            logger.error(f"Failed to get database names: {str(e)}")
            return []

    def _get_directory_databases(self) -> List[str]:
        """Get databases from directory structure."""
        databases = []
        
        try:
            # List subdirectories in base path
            for item in self.file_handler.list_directory(self.base_path):
                if self.file_handler.is_directory(item):
                    # Use directory name as database name
                    db_name = os.path.basename(item)
                    databases.append(db_name)
            
            # If no subdirectories, use base path as single database
            if not databases:
                databases = ['default']
                
        except Exception as e:
            logger.error(f"Error getting directory databases: {str(e)}")
            
        return databases

    def _get_partition_databases(self) -> List[str]:
        """Get databases from partitioned structure."""
        databases = set()
        
        try:
            # Scan for partition patterns
            all_files = self.file_handler.find_files(self.base_path, self.file_patterns, self.recursive)
            
            for file_path in all_files:
                partitions = self._extract_partitions(file_path)
                if partitions:
                    # Use first partition level as database
                    databases.add(list(partitions.keys())[0] if partitions else 'default')
                    
        except Exception as e:
            logger.error(f"Error getting partition databases: {str(e)}")
            
        return list(databases) if databases else ['default']

    def get_table_names_and_types(self, database_name: str = None) -> List[Tuple[str, str]]:
        """
        Get list of files (tables) in the specified database (directory).
        """
        tables = []
        
        try:
            # Determine search path
            if database_name and database_name != 'default':
                search_path = os.path.join(self.base_path, database_name)
            else:
                search_path = self.base_path
            
            # Find all matching files
            matching_files = self.file_handler.find_files(search_path, self.file_patterns, self.recursive)
            
            for file_path in matching_files:
                # Check if file size is within limits
                if self.file_handler.get_file_size(file_path) > self.max_file_size:
                    logger.warning(f"Skipping large file: {file_path}")
                    continue
                
                # Get file format
                file_format = self._get_file_format(file_path)
                if file_format not in self.supported_formats:
                    logger.debug(f"Skipping unsupported format: {file_path}")
                    continue
                
                # Use relative path as table name
                table_name = os.path.relpath(file_path, search_path)
                table_type = file_format.upper()
                
                tables.append((table_name, table_type))
                
        except Exception as e:
            logger.error(f"Failed to get table names for database '{database_name}': {str(e)}")
            
        return tables

    def get_table_metadata(self, table_name: str, database_name: str = None) -> Dict[str, Any]:
        """
        Extract metadata from a specific file.
        """
        try:
            # Construct full file path
            if database_name and database_name != 'default':
                file_path = os.path.join(self.base_path, database_name, table_name)
            else:
                file_path = os.path.join(self.base_path, table_name)
            
            # Get file information
            file_info = self.file_handler.get_file_info(file_path)
            
            # Parse file for schema and sample data
            parser = ParserFactory.get_parser(file_path, self.file_handler)
            schema = parser.get_schema()
            sample_data = parser.get_sample_data(limit=100)
            
            # Extract partitions if enabled
            partitions = {}
            if self.enable_partitioning:
                partitions = self._extract_partitions(file_path)
            
            # Build metadata
            metadata = {
                'file_info': file_info,
                'schema': schema,
                'sample_data': sample_data,
                'partitions': partitions,
                'format': self._get_file_format(file_path),
                'record_count': parser.get_record_count() if hasattr(parser, 'get_record_count') else None
            }
            
            return metadata
            
        except Exception as e:
            logger.error(f"Failed to extract metadata for table '{table_name}': {str(e)}")
            return {}

    def _get_file_format(self, file_path: str) -> str:
        """Determine file format from file extension."""
        _, ext = os.path.splitext(file_path.lower())
        ext = ext.lstrip('.')
        
        # Map extensions to formats
        format_mapping = {
            'csv': 'csv',
            'tsv': 'tsv',
            'txt': 'text',
            'json': 'json',
            'jsonl': 'jsonl',
            'ndjson': 'jsonl',
            'parquet': 'parquet',
            'avro': 'avro',
            'orc': 'orc',
            'xlsx': 'excel',
            'xls': 'excel',
            'feather': 'feather',
            'h5': 'hdf5',
            'hdf5': 'hdf5',
            'pkl': 'pickle',
            'pickle': 'pickle'
        }
        
        return format_mapping.get(ext, 'unknown')

    def _extract_partitions(self, file_path: str) -> Dict[str, str]:
        """Extract partition information from file path."""
        import re
        
        partitions = {}
        
        try:
            # Use regex pattern to find partitions
            matches = re.findall(self.partition_pattern, file_path)
            for match in matches:
                if len(match) == 2:
                    key, value = match
                    partitions[key] = value
                    
        except Exception as e:
            logger.debug(f"Failed to extract partitions from {file_path}: {str(e)}")
            
        return partitions

    def get_supported_formats(self) -> List[str]:
        """Return list of supported file formats."""
        return self.supported_formats

    def close(self):
        """Clean up file system connections."""
        if hasattr(self.file_handler, 'disconnect'):
            self.file_handler.disconnect()


# File System Handlers

class BaseFileHandler:
    """Base class for file system handlers."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
    
    def connect(self):
        """Establish connection to file system."""
        pass
    
    def disconnect(self):
        """Close connection to file system."""
        pass
    
    def path_exists(self, path: str) -> bool:
        """Check if path exists."""
        raise NotImplementedError
    
    def is_directory(self, path: str) -> bool:
        """Check if path is a directory."""
        raise NotImplementedError
    
    def list_directory(self, path: str) -> List[str]:
        """List contents of directory."""
        raise NotImplementedError
    
    def find_files(self, path: str, patterns: List[str], recursive: bool) -> List[str]:
        """Find files matching patterns."""
        raise NotImplementedError
    
    def get_file_size(self, path: str) -> int:
        """Get file size in bytes."""
        raise NotImplementedError
    
    def get_file_info(self, path: str) -> Dict[str, Any]:
        """Get comprehensive file information."""
        raise NotImplementedError


class LocalFileHandler(BaseFileHandler):
    """Handler for local file system."""
    
    def path_exists(self, path: str) -> bool:
        return os.path.exists(path)
    
    def is_directory(self, path: str) -> bool:
        return os.path.isdir(path)
    
    def list_directory(self, path: str) -> List[str]:
        return [os.path.join(path, item) for item in os.listdir(path)]
    
    def find_files(self, path: str, patterns: List[str], recursive: bool) -> List[str]:
        files = []
        for pattern in patterns:
            search_pattern = os.path.join(path, '**', pattern) if recursive else os.path.join(path, pattern)
            files.extend(glob.glob(search_pattern, recursive=recursive))
        return files
    
    def get_file_size(self, path: str) -> int:
        return os.path.getsize(path)
    
    def get_file_info(self, path: str) -> Dict[str, Any]:
        stat = os.stat(path)
        return {
            'size': stat.st_size,
            'modified_time': stat.st_mtime,
            'created_time': stat.st_ctime,
            'path': path,
            'name': os.path.basename(path)
        }


class FTPFileHandler(BaseFileHandler):
    """Handler for FTP file systems."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.host = config.get('host')
        self.username = config.get('username')
        self.password = config.get('password')
        self.port = config.get('port', 21)
        self.ftp = None
    
    def connect(self):
        import ftplib
        self.ftp = ftplib.FTP()
        self.ftp.connect(self.host, self.port)
        self.ftp.login(self.username, self.password)
    
    def disconnect(self):
        if self.ftp:
            self.ftp.quit()
    
    def path_exists(self, path: str) -> bool:
        try:
            self.ftp.size(path)
            return True
        except:
            try:
                self.ftp.cwd(path)
                return True
            except:
                return False
    
    def is_directory(self, path: str) -> bool:
        try:
            current = self.ftp.pwd()
            self.ftp.cwd(path)
            self.ftp.cwd(current)
            return True
        except:
            return False
    
    def list_directory(self, path: str) -> List[str]:
        return self.ftp.nlst(path)
    
    def find_files(self, path: str, patterns: List[str], recursive: bool) -> List[str]:
        # Implementation for FTP file finding
        files = []
        # Add FTP-specific file finding logic
        return files
    
    def get_file_size(self, path: str) -> int:
        return self.ftp.size(path)
    
    def get_file_info(self, path: str) -> Dict[str, Any]:
        return {
            'size': self.get_file_size(path),
            'path': path,
            'name': os.path.basename(path)
        }


class SFTPFileHandler(BaseFileHandler):
    """Handler for SFTP file systems."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.host = config.get('host')
        self.username = config.get('username')
        self.password = config.get('password')
        self.private_key = config.get('privateKey')
        self.port = config.get('port', 22)
        self.sftp = None
    
    def connect(self):
        import paramiko
        transport = paramiko.Transport((self.host, self.port))
        
        if self.private_key:
            private_key = paramiko.RSAKey.from_private_key_file(self.private_key)
            transport.connect(username=self.username, pkey=private_key)
        else:
            transport.connect(username=self.username, password=self.password)
        
        self.sftp = paramiko.SFTPClient.from_transport(transport)
    
    def disconnect(self):
        if self.sftp:
            self.sftp.close()
    
    def path_exists(self, path: str) -> bool:
        try:
            self.sftp.stat(path)
            return True
        except:
            return False
    
    def is_directory(self, path: str) -> bool:
        try:
            return self.sftp.stat(path).st_mode & 0o040000 != 0
        except:
            return False
    
    def list_directory(self, path: str) -> List[str]:
        return [os.path.join(path, item) for item in self.sftp.listdir(path)]
    
    def find_files(self, path: str, patterns: List[str], recursive: bool) -> List[str]:
        # Implementation for SFTP file finding
        files = []
        # Add SFTP-specific file finding logic
        return files
    
    def get_file_size(self, path: str) -> int:
        return self.sftp.stat(path).st_size
    
    def get_file_info(self, path: str) -> Dict[str, Any]:
        stat = self.sftp.stat(path)
        return {
            'size': stat.st_size,
            'modified_time': stat.st_mtime,
            'path': path,
            'name': os.path.basename(path)
        }


class HDFSFileHandler(BaseFileHandler):
    """Handler for HDFS file systems."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.namenode_host = config.get('namenodeHost')
        self.namenode_port = config.get('namenodePort', 9000)
        self.username = config.get('username')
        self.hdfs_client = None
    
    def connect(self):
        try:
            import hdfs
            self.hdfs_client = hdfs.InsecureClient(
                f"http://{self.namenode_host}:{self.namenode_port}",
                user=self.username
            )
        except ImportError:
            raise ImportError("hdfs library required for HDFS support: pip install hdfs")
    
    def path_exists(self, path: str) -> bool:
        try:
            self.hdfs_client.status(path)
            return True
        except:
            return False
    
    def is_directory(self, path: str) -> bool:
        try:
            status = self.hdfs_client.status(path)
            return status['type'] == 'DIRECTORY'
        except:
            return False
    
    def list_directory(self, path: str) -> List[str]:
        return [os.path.join(path, item) for item in self.hdfs_client.list(path)]
    
    def find_files(self, path: str, patterns: List[str], recursive: bool) -> List[str]:
        # Implementation for HDFS file finding
        files = []
        # Add HDFS-specific file finding logic
        return files
    
    def get_file_size(self, path: str) -> int:
        return self.hdfs_client.status(path)['length']
    
    def get_file_info(self, path: str) -> Dict[str, Any]:
        status = self.hdfs_client.status(path)
        return {
            'size': status['length'],
            'modified_time': status['modificationTime'] / 1000,  # Convert from milliseconds
            'path': path,
            'name': os.path.basename(path),
            'owner': status['owner'],
            'group': status['group'],
            'permissions': status['permission']
        }


class CloudFileHandler(BaseFileHandler):
    """Generic handler for cloud storage services."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.cloud_type = config.get('cloudType', 'generic')  # generic, cloud-a, cloud-b, cloud-c
        self.container_name = config.get('containerName')
        self.credentials = config.get('credentials', {})
        self.client = None
    
    def connect(self):
        if self.cloud_type == 'cloud-a':
            self._connect_cloud_a()
        elif self.cloud_type == 'cloud-b':
            self._connect_cloud_b()
        elif self.cloud_type == 'cloud-c':
            self._connect_cloud_c()
        else:
            raise ValueError(f"Unsupported cloud type: {self.cloud_type}")
    
    def _connect_cloud_a(self):
        # Implement Cloud Provider A connection
        # import cloud_a_sdk
        # self.client = cloud_a_sdk.Client(**self.credentials)
        pass
    
    def _connect_cloud_b(self):
        # Implement Cloud Provider B connection
        # import cloud_b_sdk
        # self.client = cloud_b_sdk.Client(**self.credentials)
        pass
    
    def _connect_cloud_c(self):
        # Implement Cloud Provider C connection
        # import cloud_c_sdk
        # self.client = cloud_c_sdk.Client(**self.credentials)
        pass
    
    def path_exists(self, path: str) -> bool:
        # Implementation varies by cloud provider
        if self.cloud_type == 'cloud-a':
            # Implement Cloud Provider A path existence check
            # return self.client.object_exists(container=self.container_name, path=path)
            pass
        # Add implementations for other cloud providers
        return False
    
    def is_directory(self, path: str) -> bool:
        # Cloud storage doesn't have true directories, but we can check for prefix
        return path.endswith('/')
    
    def list_directory(self, path: str) -> List[str]:
        # Implementation varies by cloud provider
        files = []
        if self.cloud_type == 'cloud-a':
            # Implement Cloud Provider A directory listing
            # response = self.client.list_objects(container=self.container_name, prefix=path)
            # files.extend(response.get('contents', []))
            pass
        # Add implementations for other cloud providers
        return files
    
    def find_files(self, path: str, patterns: List[str], recursive: bool) -> List[str]:
        # Implementation for cloud file finding
        files = []
        # Add cloud-specific file finding logic
        return files
    
    def get_file_size(self, path: str) -> int:
        if self.cloud_type == 'cloud-a':
            # Implement Cloud Provider A file size retrieval
            # response = self.client.get_object_metadata(container=self.container_name, path=path)
            # return response['size']
            pass
        return 0
    
    def get_file_info(self, path: str) -> Dict[str, Any]:
        if self.cloud_type == 'cloud-a':
            # Implement Cloud Provider A file info retrieval
            # response = self.client.get_object_metadata(container=self.container_name, path=path)
            # return {
            #     'size': response['size'],
            #     'modified_time': response['last_modified'],
            #     'path': path,
            #     'name': os.path.basename(path),
            #     'etag': response['etag']
            # }
            pass
        return {}
