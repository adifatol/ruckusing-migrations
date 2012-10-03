<?php

class Ruckusing_BaseAdapter {
	protected $dsn;
	protected $db;
	protected $conn;
	protected $tables_loaded = false;

	function __construct($dsn, $logger) {

		$this->connect($dsn);
		$this->set_logger($logger);

		$this->set_dsn($dsn);
	}

	//-----------------------------------
	// PRIVATE METHODS
	//-----------------------------------
	protected function connect($dsn) {
		$this->db_connect($dsn);
	}

	//-------- DATABASE LEVEL OPERATIONS
	public function database_exists($db) {
		$ddl = "SHOW DATABASES";
		$result = $this->select_all($ddl);
		if(count($result) == 0) {
			return false;
		}
		foreach($result as $dbrow) {
			if($dbrow['Database'] == $db) {
				return true;
			}
		}
		return false;
	}
	public function create_database($db) {
		if($this->database_exists($db)) {
			return false;
		}
		$ddl = sprintf("CREATE DATABASE %s", $this->identifier($db));
		$result = $this->query($ddl);
		return($result === true);
	}
	
	public function drop_database($db) {
		if(!$this->database_exists($db)) {
			return false;
		}
		$ddl = sprintf("DROP DATABASE IF EXISTS %s", $this->identifier($db));
		$result = $this->query($ddl);
		return($result === true);
	}
	
	public function table_exists($tbl, $reload_tables = false) {
		$this->load_tables($reload_tables);
		return array_key_exists($tbl, $this->tables);
	}
	
	public function show_fields_from($tbl) {
		return "";
	}
	
	public function quote_string($str) {
		return mysql_real_escape_string($str);
	}

	public function get_database_name() {
		$db_info = $this->get_dsn();
		return($db_info['database']);
	}

	public function supports_migrations() {
		return true;
	}
	
	public function set_dsn($dsn) { 
		$this->dsn = $dsn;
	}

	public function get_dsn() {
		return $this->dsn;
	}

	public function set_logger($logger) {
		$this->logger = $logger;
	}

	public function get_logger($logger) {
		return $this->logger;
	}
	
	//alias
	public function has_table($tbl) {
		return $this->table_exists($tbl);
	}

	public function rename_table($name, $new_name) {
		if(empty($name)) {
			throw new Ruckusing_ArgumentException("Missing original column name parameter");
		}
		if(empty($new_name)) {
			throw new Ruckusing_ArgumentException("Missing new column name parameter");
		}
		$sql = sprintf("RENAME TABLE %s TO %s", $this->identifier($name), $this->identifier($new_name));
		return $this->execute_ddl($sql);
	}//create_table
	
	public function add_column($table_name, $column_name, $type, $options = array()) {
		if(empty($table_name)) {
			throw new Ruckusing_ArgumentException("Missing table name parameter");
		}
		if(empty($column_name)) {
			throw new Ruckusing_ArgumentException("Missing column name parameter");
		}
		if(empty($type)) {
			throw new Ruckusing_ArgumentException("Missing type parameter");
		}
		//default types
		if(!array_key_exists('limit', $options)) {
			$options['limit'] = null;
		}
		if(!array_key_exists('precision', $options)) {
			$options['precision'] = null;
		}
		if(!array_key_exists('scale', $options)) {
			$options['scale'] = null;
		}
		$sql = sprintf("ALTER TABLE %s ADD %s %s", $this->identifier($table_name), $this->identifier($column_name), $this->type_to_sql($type,$options));
		$sql .= $this->add_column_options($type, $options);
		return $this->execute_ddl($sql);
	}//add_column
	
	public function remove_column($table_name, $column_name) {
		$sql = sprintf("ALTER TABLE %s DROP COLUMN %s", $this->identifier($table_name), $this->identifier($column_name));
		return $this->execute_ddl($sql);
	}//remove_column
	
	public function rename_column($table_name, $column_name, $new_column_name) {
		if(empty($table_name)) {
			throw new Ruckusing_ArgumentException("Missing table name parameter");
		}
		if(empty($column_name)) {
			throw new Ruckusing_ArgumentException("Missing original column name parameter");
		}
		if(empty($new_column_name)) {
			throw new Ruckusing_ArgumentException("Missing new column name parameter");
		}
		$column_info = $this->column_info($table_name, $column_name);
		$current_type = $column_info['type'];
		$sql =  sprintf("ALTER TABLE %s CHANGE %s %s %s",
				$this->identifier($table_name),
				$this->identifier($column_name),
				$this->identifier($new_column_name), $current_type);
		return $this->execute_ddl($sql);
	}//rename_column
	
	public function change_column($table_name, $column_name, $type, $options = array()) {
		if(empty($table_name)) {
			throw new Ruckusing_ArgumentException("Missing table name parameter");
		}
		if(empty($column_name)) {
			throw new Ruckusing_ArgumentException("Missing original column name parameter");
		}
		if(empty($type)) {
			throw new Ruckusing_ArgumentException("Missing type parameter");
		}
		$column_info = $this->column_info($table_name, $column_name);
		//default types
		if(!array_key_exists('limit', $options)) {
			$options['limit'] = null;
		}
		if(!array_key_exists('precision', $options)) {
			$options['precision'] = null;
		}
		if(!array_key_exists('scale', $options)) {
			$options['scale'] = null;
		}
		$sql = sprintf("ALTER TABLE %s CHANGE %s %s %s", $this->identifier($table_name), $this->identifier($column_name), $this->identifier($column_name),  $this->type_to_sql($type,$options));
		$sql .= $this->add_column_options($type, $options);
		return $this->execute_ddl($sql);
	}//change_column
	
	public function column_info($table, $column) {
		if(empty($table)) {
			throw new Ruckusing_ArgumentException("Missing table name parameter");
		}
		if(empty($column)) {
			throw new Ruckusing_ArgumentException("Missing original column name parameter");
		}
		try {
			$sql = sprintf("SHOW COLUMNS FROM %s LIKE '%s'", $this->identifier($table), $column);
			$result = $this->select_one($sql);
			if(is_array($result)) {
				//lowercase key names
				$result = array_change_key_case($result, CASE_LOWER);
			}
			return $result;
		}catch(Exception $e) {
			return null;
		}
	}//column_info
	
	public function add_index($table_name, $column_name, $options = array()) {
		if(empty($table_name)) {
			throw new Ruckusing_ArgumentException("Missing table name parameter");
		}
		if(empty($column_name)) {
			throw new Ruckusing_ArgumentException("Missing column name parameter");
		}
		//unique index?
		if(is_array($options) && array_key_exists('unique', $options) && $options['unique'] === true) {
			$unique = true;
		} else {
			$unique = false;
		}
		//did the user specify an index name?
		if(is_array($options) && array_key_exists('name', $options)) {
			$index_name = $options['name'];
		} else {
			$index_name = Ruckusing_NamingUtil::index_name($table_name, $column_name);
		}
	
		if(strlen($index_name) > MAX_IDENTIFIER_LENGTH) {
			$msg = "The auto-generated index name is too long for MySQL (max is 64 chars). ";
			$msg .= "Considering using 'name' option parameter to specify a custom name for this index.";
			$msg .= " Note: you will also need to specify";
			$msg .= " this custom name in a drop_index() - if you have one.";
			throw new Ruckusing_InvalidIndexNameException($msg);
		}
		if(!is_array($column_name)) {
			$column_names = array($column_name);
		} else {
			$column_names = $column_name;
		}
		$cols = array();
		foreach($column_names as $name) {
			$cols[] = $this->identifier($name);
		}
		$sql = sprintf("CREATE %sINDEX %s ON %s(%s)",
				$unique ? "UNIQUE " : "",
				$index_name,
				$this->identifier($table_name),
				join(", ", $cols));
		return $this->execute_ddl($sql);
	}//add_index
	
	public function remove_index($table_name, $column_name, $options = array()) {
		if(empty($table_name)) {
			throw new Ruckusing_ArgumentException("Missing table name parameter");
		}
		if(empty($column_name)) {
			throw new Ruckusing_ArgumentException("Missing column name parameter");
		}
		//did the user specify an index name?
		if(is_array($options) && array_key_exists('name', $options)) {
			$index_name = $options['name'];
		} else {
			$index_name = Ruckusing_NamingUtil::index_name($table_name, $column_name);
		}
		$sql = sprintf("DROP INDEX %s ON %s", $this->identifier($index_name), $this->identifier($table_name));
		return $this->execute_ddl($sql);
	}
	
	public function has_index($table_name, $column_name, $options = array()) {
		if(empty($table_name)) {
			throw new Ruckusing_ArgumentException("Missing table name parameter");
		}
		if(empty($column_name)) {
			throw new Ruckusing_ArgumentException("Missing column name parameter");
		}
		//did the user specify an index name?
		if(is_array($options) && array_key_exists('name', $options)) {
			$index_name = $options['name'];
		} else {
			$index_name = Ruckusing_NamingUtil::index_name($table_name, $column_name);
		}
		$indexes = $this->indexes($table_name);
		foreach($indexes as $idx) {
			if($idx['name'] == $index_name) {
				return true;
			}
		}
		return false;
	}//has_index
	
	public function indexes($table_name) {
		$sql = sprintf("SHOW KEYS FROM %s", $this->identifier($table_name));
		$result = $this->select_all($sql);
		$indexes = array();
		$cur_idx = null;
		foreach($result as $row) {
			//skip primary
			if($row['Key_name'] == 'PRIMARY') {
				continue;
			}
			$cur_idx = $row['Key_name'];
			$indexes[] = array('name' => $row['Key_name'], 'unique' => (int)$row['Non_unique'] == 0 ? true : false);
		}
		return $indexes;
	}//has_index

	//transaction methods
	public function start_transaction() {
		try {
			if($this->inTransaction() === false) {
				$this->beginTransaction();
			}
		}catch(Exception $e) {
			trigger_error($e->getMessage());
		}
	}
	public function commit_transaction() {
		try {
			if($this->inTransaction()) {
				$this->commit();
			}
		}catch(Exception $e) {
			trigger_error($e->getMessage());
		}
	}
	public function rollback_transaction() {
		try {
			if($this->inTransaction()) {
				$this->rollback();
			}
		}catch(Exception $e) {
			trigger_error($e->getMessage());
		}
	}

	/* Create the schema table, if necessary */
	public function create_schema_version_table() {
		if(!$this->has_table(RUCKUSING_TS_SCHEMA_TBL_NAME)) {
			$t = $this->create_table(RUCKUSING_TS_SCHEMA_TBL_NAME, array('id' => false));
			$t->column('version', 'string');
			$t->finish();
			$this->add_index(RUCKUSING_TS_SCHEMA_TBL_NAME, 'version', array('unique' => true));
		}//if !has_table
	}
	
	public function create_table($table_name, $options = array()) {
		return new Ruckusing_MySQLTableDefinition($this, $table_name, $options);
	}

	//;$limit = null, $precision = null, $scale = null
	public function type_to_sql($type, $options = array()) {
		$natives = $this->native_database_types();
	
		if(!array_key_exists($type, $natives)) {
			$error = sprintf("Error:I dont know what column type of '%s' maps to for MySQL.", $type);
			$error .= "\nYou provided: {$type}\n";
			$error .= "Valid types are: \n";
			$types = array_keys($natives);
			foreach($types as $t) {
				if($t == 'primary_key') {
					continue;
				}
				$error .= "\t{$t}\n";
			}
			throw new Ruckusing_ArgumentException($error);
		}
		 
		$scale = null;
		$precision = null;
		$limit = null;
		 
		if(isset($options['precision'])) {
			$precision = $options['precision'];
		}
		if(isset($options['scale'])) {
			$scale = $options['scale'];
		}
		if(isset($options['limit'])) {
			$limit = $options['limit'];
		}

		$native_type = $natives[$type];
		if( is_array($native_type) && array_key_exists('name', $native_type)) {
			$column_type_sql = $native_type['name'];
		} else {
			return $native_type;
		}
		if($type == "decimal") {
			//ignore limit, use precison and scale
			if( $precision == null && array_key_exists('precision', $native_type)) {
				$precision = $native_type['precision'];
			}
			if( $scale == null && array_key_exists('scale', $native_type)) {
				$scale = $native_type['scale'];
			}
			if($precision != null) {
				if(is_int($scale)) {
					$column_type_sql .= sprintf("(%d, %d)", $precision, $scale);
				} else {
					$column_type_sql .= sprintf("(%d)", $precision);
				}//scale
			} else {
				if($scale) {
					throw new Ruckusing_ArgumentException("Error adding decimal column: precision cannot be empty if scale is specified");
				}
			}//precision
		} elseif($type == "float") {
			//ignore limit, use precison and scale
			if( $precision == null && array_key_exists('precision', $native_type)) {
				$precision = $native_type['precision'];
			}
			if( $scale == null && array_key_exists('scale', $native_type)) {
				$scale = $native_type['scale'];
			}
			if($precision != null) {
				if(is_int($scale)) {
					$column_type_sql .= sprintf("(%d, %d)", $precision, $scale);
				} else {
					$column_type_sql .= sprintf("(%d)", $precision);
				}//scale
			} else {
				if ($scale) {
					throw new Ruckusing_ArgumentException("Error adding float column: precision cannot be empty if scale is specified");
				}
			}//precision
		}  {
			//not a decimal column
			if($limit == null && array_key_exists('limit', $native_type)) {
				$limit = $native_type['limit'];
			}
			if($limit) {
				$column_type_sql .= sprintf("(%d)", $limit);
			}
		}
		return $column_type_sql;
	}//type_to_sql

	public function add_column_options($type, $options) {
		$sql = "";
	
		if(!is_array($options))
			return $sql;
	
		if(array_key_exists('unsigned', $options) && $options['unsigned'] === true) {
			$sql .= ' UNSIGNED';
		}
	
		if(array_key_exists('auto_increment', $options) && $options['auto_increment'] === true) {
			$sql .= ' auto_increment';
		}
	
		if(array_key_exists('default', $options) && $options['default'] !== null) {
			if($this->is_sql_method_call($options['default'])) {
				//$default_value = $options['default'];
				throw new Exception("MySQL does not support function calls as default values, constants only.");
			}
	
			if(is_int($options['default'])) {
				$default_format = '%d';
			} elseif(is_bool($options['default'])) {
				$default_format = "'%d'";
			} else {
				$default_format = "'%s'";
			}
			$default_value = sprintf($default_format, $options['default']);
	
			$sql .= sprintf(" DEFAULT %s", $default_value);
		}
	
		if(array_key_exists('null', $options) && $options['null'] === false) {
			$sql .= " NOT NULL";
		}
		if(array_key_exists('comment', $options)) {
			$sql .= sprintf(" COMMENT '%s'", $this->quote_string($options['comment']));
		}
		if(array_key_exists('after', $options)) {
			$sql .= sprintf(" AFTER %s", $this->identifier($options['after']));
		}
	
		return $sql;
	}//add_column_options
	
	public function set_current_version($version) {
		$sql = sprintf("INSERT INTO %s (version) VALUES ('%s')", RUCKUSING_TS_SCHEMA_TBL_NAME, $version);
		return $this->execute_ddl($sql);
	}
	
	public function remove_version($version) {
		$sql = sprintf("DELETE FROM %s WHERE version = '%s'", RUCKUSING_TS_SCHEMA_TBL_NAME, $version);
		return $this->execute_ddl($sql);
	}
	
	public function __toString() {
		return "Ruckusing_MySQLAdapter, version " . $this->version;
	}

	
	public function select_one($query) {
		$this->logger->log($query);
		$query_type = $this->determine_query_type($query);
		if($query_type == SQL_SELECT || $query_type == SQL_SHOW) {
			$res = mysql_query($query, $this->conn);
			if($this->isError($res)) {
				trigger_error(sprintf("Error executing 'query' with:\n%s\n\nReason: %s\n\n", $query, mysql_error($this->conn)));
			}
			return mysql_fetch_assoc($res);
		} else {
			trigger_error("Query for select_one() is not one of SELECT or SHOW: $query");
		}
	}

	public function select_all($query) {
		return $this->query($query);
	}

	public function execute($query) {
		return $this->query($query);
	}

	protected function determine_query_type($query) {
		$query = strtolower(trim($query));
	
		if(preg_match('/^select/', $query)) {
			return SQL_SELECT;
		}
		if(preg_match('/^update/', $query)) {
			return SQL_UPDATE;
		}
		if(preg_match('/^delete/', $query)) {
			return SQL_DELETE;
		}
		if(preg_match('/^insert/', $query)) {
			return SQL_INSERT;
		}
		if(preg_match('/^alter/', $query)) {
			return SQL_ALTER;
		}
		if(preg_match('/^drop/', $query)) {
			return SQL_DROP;
		}
		if(preg_match('/^create/', $query)) {
			return SQL_CREATE;
		}
		if(preg_match('/^show/', $query)) {
			return SQL_SHOW;
		}
		if(preg_match('/^rename/', $query)) {
			return SQL_RENAME;
		}
		if(preg_match('/^set/', $query)) {
			return SQL_SET;
		}
		// else
		return SQL_UNKNOWN_QUERY_TYPE;
	}
	
	protected function is_select($query_type) {
		if($query_type == SQL_SELECT) {
			return true;
		}
		return false;
	}
	
	public function drop_table($tbl) {
		$ddl = sprintf("DROP TABLE IF EXISTS %s", $this->identifier($tbl));
		$result = $this->query($ddl);
		return true;
	}

	/*
	 Use this method for non-SELECT queries
	Or anything where you dont necessarily expect a result string, e.g. DROPs, CREATEs, etc.
	*/
	public function execute_ddl($ddl) {
		$result = $this->query($ddl);
		return true;
	
	}

	/*
	 Detect whether or not the string represents a function call and if so
	do not wrap it in single-quotes, otherwise do wrap in single quotes.
	*/
	protected function is_sql_method_call($str) {
		$str = trim($str);
		if(substr($str, -2, 2) == "()") {
			return true;
		} else {
			return false;
		}
	}
}
?>
