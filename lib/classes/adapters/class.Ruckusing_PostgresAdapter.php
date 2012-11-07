<?php

require_once RUCKUSING_BASE . '/lib/classes/class.Ruckusing_BaseAdapter.php';
require_once RUCKUSING_BASE . '/lib/classes/class.Ruckusing_iAdapter.php';
require_once RUCKUSING_BASE . '/lib/classes/util/class.Ruckusing_NamingUtil.php';
require_once RUCKUSING_BASE . '/lib/classes/adapters/class.Ruckusing_PostgresTableDefinition.php';

class Ruckusing_PostgresAdapter extends Ruckusing_BaseAdapter implements Ruckusing_iAdapter {

	protected $name = "Postgres";
	protected $tables = array();
	protected $version = '1.0';
	protected $in_trx = false;

	/*
	  @TODO: allow for the date/time types to accept a precision value
	 */

	public function native_database_types() {
		$types = array(
			'primary_key'	=> "serial",
			'serial'		=> array('name' => "serial",),
			'string'		=> array('name' => "varchar", 'limit' => 255),
			'text'			=> array('name' => "text",),
			'integer'		=> array('name' => "integer",),
			'float'			=> array('name' => "real",),
			'decimal'		=> array('name' => "decimal",),
			'datetime'		=> array('name' => "timestamp",),
			'timestamp'		=> array('name' => "timestamp",),
			'time'			=> array('name' => "time",),
			'date'			=> array('name' => "date",),
			'binary'		=> array('name' => "bytea",),
			'boolean'		=> array('name' => "boolean",)
		);
		return $types;
	}

	public function identifier($str) {
		return('"' . $str . '"');
	}

	public function get_database_name() {
		return;
	}

	public function quote($value, $column) {
		return;
	}
	
	public function schema(){
		throw new Exception('Not implemented yet.');
	}

	// Initialize an array of table names
	protected function load_tables($reload = true) {
		$db_info = $this->get_dsn();
		if($this->tables_loaded == false || $reload) {
			$this->tables = array(); //clear existing structure
			$qry = "SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('pg_catalog', 'information_schema');";
			$res = pg_query($this->conn,$qry);
			while($row = pg_fetch_assoc($res)) {
				$table = $row['table_name'];
				$this->tables[$table] = true;
			}
		}
	}

	public function create_table($table_name, $options = array()) {
		return new Ruckusing_PostgresTableDefinition($this, $table_name, $options);
	}

	public function execute($query){
		$this->query($query);
	}

	public function query($query) {
		$this->logger->log($query);
		$query_type = $this->determine_query_type($query);
		$data = array();

		if ($query_type == SQL_SELECT || $query_type == SQL_SHOW) {
			$res = pg_query($this->conn, $query);
			if (!$res) {
				trigger_error(sprintf("Error executing 'query' with:\n%s\n\nReason: %s\n\n", $query, pg_last_error($this->conn)));
			}
			while ($row = pg_fetch_assoc($res)) {
				$data[] = $row;
			}
			return $data;
		} else {
			// INSERT, DELETE, etc...
			try {

				$res = pg_query($this->conn, $query);

				if (!$res) {
					trigger_error(sprintf("Error executing 'query' with:\n%s\n\nReason: %s\n\n", $query, pg_last_error($this->conn)));
				}
			} catch (\Exception $e){
				trigger_error(sprintf("Error executing 'query' with:\n%s\n\nReason: %s\n\n", $query, $e->getMessage()));
			}

			if ($query_type == SQL_INSERT) {
				return pg_getlastoid($res);
				//return mysql_insert_id($this->conn);
			}

			return true;
		}
	}

	protected function db_connect($dsn) {
		$db_info = $dsn;
		if ($db_info) {
			$this->db_info = $db_info;
			//we might have a port
			if (empty($db_info['port'])) {
				die("\n\nCould not connect to the DB, please set the port\n\n");
			}
			$this->conn = pg_connect("host={$db_info['host']} port={$db_info['port']} dbname={$db_info['database']} user={$db_info['user']} password={$db_info['password']}"); //$host, $db_info['user'], $db_info['password']);
			
			if (!$this->conn) {
				die("\n\nCould not connect to the DB, check host / database / user / password\n\n");
			}
			return true;
		} else {
			die("\n\nCould not extract DB connection information from: {$dsn}\n\n");
		}
	}
	
	protected function db_disconnect(){
		pg_close($this->conn);
	}

	//Delegate to PEAR
	protected function isError($o) {
		return $o === FALSE;
	}

	protected function determine_query_type($query) {
		$query = strtolower(trim($query));

		if (preg_match('/^select/', $query)) {
			return SQL_SELECT;
		}
		if (preg_match('/^update/', $query)) {
			return SQL_UPDATE;
		}
		if (preg_match('/^delete/', $query)) {
			return SQL_DELETE;
		}
		if (preg_match('/^insert/', $query)) {
			return SQL_INSERT;
		}
		if (preg_match('/^alter/', $query)) {
			return SQL_ALTER;
		}
		if (preg_match('/^drop/', $query)) {
			return SQL_DROP;
		}
		if (preg_match('/^create/', $query)) {
			return SQL_CREATE;
		}
		if (preg_match('/^show/', $query)) {
			return SQL_SHOW;
		}
		if (preg_match('/^rename/', $query)) {
			return SQL_RENAME;
		}
		if (preg_match('/^set/', $query)) {
			return SQL_SET;
		}
		// else
		return SQL_UNKNOWN_QUERY_TYPE;
	}

	protected function is_select($query_type) {
		if ($query_type == SQL_SELECT) {
			return true;
		}
		return false;
	}

	/*
	  Detect whether or not the string represents a function call and if so
	  do not wrap it in single-quotes, otherwise do wrap in single quotes.
	 */

	protected function is_sql_method_call($str) {
		$str = trim($str);
		if (substr($str, -2, 2) == "()") {
			return true;
		} else {
			return false;
		}
	}

	protected function inTransaction() {
		return $this->in_trx;
	}

	protected function beginTransaction() {
		pg_query($this->conn, "BEGIN WORK");
		$this->in_trx = true;
	}

	protected function commit() {
		if ($this->in_trx === true) {
			pg_query($this->conn, "COMMIT");
			$this->in_trx = false;
		}
	}

	protected function rollback() {
		if ($this->in_trx === true) {
			pg_query($this->conn, "ROLLBACK");
			$this->in_trx = false;
		}
	}
	
	public function select_one($query) {
		$this->logger->log($query);
		$query_type = $this->determine_query_type($query);
		if($query_type == SQL_SELECT || $query_type == SQL_SHOW) {
			$res = pg_query($this->conn, $query);
			if($this->isError($res)) {
				trigger_error(sprintf("Error executing 'query' with:\n%s\n\nReason: %s\n\n", $query, pg_last_error($this->conn)));
			}
			return pg_fetch_assoc($res);
		} else {
			trigger_error("Query for select_one() is not one of SELECT or SHOW: $query");
		}
	}
	
	public function column_info($table, $column) {
		if(empty($table)) {
			throw new Ruckusing_ArgumentException("Missing table name parameter");
		}
		if(empty($column)) {
			throw new Ruckusing_ArgumentException("Missing original column name parameter");
		}
		try {
			$sql = sprintf("SELECT * FROM  information_schema.COLUMNS WHERE table_name = '%s' AND column_name='%s'", $table, $column);
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
		$type_to_sql = $this->type_to_sql($type,$options);
		
		$sql = sprintf("ALTER TABLE %s ALTER COLUMN %s", $this->identifier($table_name), $this->identifier($column_name), $this->identifier($table_name), $this->identifier($column_name));
		$alter_sql = sprintf("{$sql} TYPE %s;", $type_to_sql);
		$alter_sql .= $this->change_column_options($sql, $type, $options, $table_name, $column_name);
		return $this->execute_ddl($alter_sql);
	}//change_column
	
	public function change_column_options($pSql, $type, $options, $tableName, $columnName) {
		$sql = "";
	
		if(!is_array($options))
			return $pSql;
	
		if(array_key_exists('unsigned', $options) && $options['unsigned'] === true) {
			$sql .= $pSql . ' SET UNSIGNED;';
		}
	
		if(array_key_exists('auto_increment', $options) && $options['auto_increment'] === true) {
			$sql .= $pSql . ' SET auto_increment;';
		}
	
		if(array_key_exists('default', $options)) {
			if ($options['default'] !== null){
				if($this->is_sql_method_call($options['default'])) {
					//$default_value = $options['default'];
					throw new Exception("PostgreSQL does not support function calls as default values, constants only.");
				}

				if(is_int($options['default'])) {
					$default_format = '%d';
				} elseif(is_bool($options['default'])) {
					$default_format = "'%d'";
				} else {
					$default_format = "'%s'";
				}
				$default_value = sprintf($default_format, $options['default']);

				$sql .= $pSql . sprintf(" SET DEFAULT %s;", $default_value);
			} else {
				$sql .= $pSql . " DROP DEFAULT;";
			}
		}
	
		if(array_key_exists('null', $options)){
			if ($options['null'] !== false) {
				$sql .= $pSql . " SET NOT NULL;";
			} else {
				$sql .= $pSql . " DROP NOT NULL;";
			}
		}
		/* drop comment*/
		if(array_key_exists('comment', $options)) {
			$sql .= sprintf(" COMMENT ON COLUMN %s IS '%s';", $this->identifier($tableName) . '.' . $this->identifier($columnName), $this->quote_string($options['comment']));
		} else {
			$sql .= sprintf(" COMMENT ON COLUMN %s IS NULL;", $this->identifier($tableName) . '.' . $this->identifier($columnName));
		}
	
		return $sql;
	}// change column options
}

//class Ruckusing_PostgresAdapter
?>
