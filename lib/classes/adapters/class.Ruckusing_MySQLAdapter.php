<?php

require_once RUCKUSING_BASE . '/lib/classes/class.Ruckusing_BaseAdapter.php';
require_once RUCKUSING_BASE . '/lib/classes/class.Ruckusing_iAdapter.php';
require_once RUCKUSING_BASE . '/lib/classes/adapters/class.Ruckusing_MySQLTableDefinition.php';
require_once RUCKUSING_BASE . '/lib/classes/util/class.Ruckusing_NamingUtil.php';	

define('SQL_UNKNOWN_QUERY_TYPE', 1);
define('SQL_SELECT', 2);
define('SQL_INSERT', 4);
define('SQL_UPDATE', 8);
define('SQL_DELETE', 16);
define('SQL_ALTER', 32);
define('SQL_DROP', 64);
define('SQL_CREATE', 128);
define('SQL_SHOW', 256);
define('SQL_RENAME', 512);
define('SQL_SET', 1024);

define('MAX_IDENTIFIER_LENGTH', 64); // max length of an identifier like a column or index name


class Ruckusing_MySQLAdapter extends Ruckusing_BaseAdapter implements Ruckusing_iAdapter {

	protected $name = "MySQL";
	protected $tables = array();
	protected $version = '1.0';
	protected $in_trx = false;

	public function native_database_types() {
		$types = array(
      'primary_key'   => array('name' => 'integer', 'limit' => 11, 'null' => false),
      'string'        => array('name' => "varchar", 	'limit' 		=> 255),
      'text'          => array('name' => "text", 												),
      'mediumtext'    => array('name' => 'mediumtext'                   ),
      'integer'       => array('name' => "int", 			'limit' 		=> 11 ),
      'smallinteger'  => array('name' => "smallint"                     ),
      'biginteger'    => array('name' => "bigint"                     ),
      'float'         => array('name' => "float"),
      'decimal'       => array('name' => "decimal", 'scale' => 10, 'precision' => 0),
      'datetime'      => array('name' => "datetime", 										),
      'timestamp'     => array('name' => "timestamp",										),
      'time'          => array('name' => "time", 												),
      'date'          => array('name' => "date", 												),
      'binary'        => array('name' => "blob", 												),
      'boolean'       => array('name' => "tinyint", 	'limit' 		=> 1  )
			);
		return $types;
	}
	
	//-----------------------------------
	// PUBLIC METHODS
	//-----------------------------------
		
  public function quote_table($str) {
    return "`" . $str . "`";
  }
	
	public function column_definition($column_name, $type, $options = null) {
		$col = new Ruckusing_ColumnDefinition($this, $column_name, $type, $options);
		return $col->__toString();
	}//column_definition

	// Initialize an array of table names
	protected function load_tables($reload = true) {
		if($this->tables_loaded == false || $reload) {
			$this->tables = array(); //clear existing structure
			$qry = "SHOW TABLES";
			$res = mysql_query($qry, $this->conn);
			while($row = mysql_fetch_row($res)) {
				$table = $row[0];
				$this->tables[$table] = true;
			}
		}
	}

	/*
		Dump the complete schema of the DB. This is really just all of the 
		CREATE TABLE statements for all of the tables in the DB.
		
		NOTE: this does NOT include any INSERT statements or the actual data
		(that is, this method is NOT a replacement for mysqldump)
	*/
	public function schema() {
		$final = "";
    $views = '';
		$this->load_tables(true);
		foreach($this->tables as $tbl => $idx) {

			if($tbl == 'schema_info') { continue; }

			$stmt = sprintf("SHOW CREATE TABLE %s", $this->identifier($tbl));
			$result = $this->query($stmt);

      if(is_array($result) && count($result) == 1) {
        $row = $result[0];
        if(count($row) == 2) {
          if (isset($row['Create Table'])) {
            $final .= $row['Create Table'] . ";\n\n";
          } else if (isset($row['Create View'])) {
            $views .= $row['Create View'] . ";\n\n";
          }
        }
      }
		}
		return $final.$views;
	}
	  
  public function identifier($str) {
    return("`" . $str . "`");
  }
	
	public function quote($value, $column) {
	  return $this->quote_string($value);
	}

	//-----------------------------------
	// PRIVATE METHODS
	//-----------------------------------	
	
  protected function db_connect($dsn) {
    $db_info = $dsn;
    if($db_info) {
      $this->db_info = $db_info;
      //we might have a port
      if(!empty($db_info['port'])) {
        $host = $db_info['host'] . ':' . $db_info['port'];
      } else {
        $host = $db_info['host'];
      }
      $this->conn = mysql_connect($host, $db_info['user'], $db_info['password']);
      if(!$this->conn) {
        die("\n\nCould not connect to the DB, check host / user / password\n\n");
      }
      if(!mysql_select_db($db_info['database'], $this->conn)) {
        die("\n\nCould not select the DB, check permissions on host\n\n");
      }
      return true;
    } else {
      die("\n\nCould not extract DB connection information from: {$dsn}\n\n");
    }
  }
  
  protected function db_disconnect(){
	  if ($this->conn){
		  mysql_close($this->conn);
	  }
  }
	
	//Delegate to PEAR
  protected function isError($o) {
    return $o === FALSE;
  }
  
  public function query($query) {
  	$this->logger->log($query);
  	$query_type = $this->determine_query_type($query);
  	$data = array();
  	if($query_type == SQL_SELECT || $query_type == SQL_SHOW) {
  		$res = mysql_query($query, $this->conn);
  		if($this->isError($res)) {
  			trigger_error(sprintf("Error executing 'query' with:\n%s\n\nReason: %s\n\n", $query, mysql_error($this->conn)));
  		}
  		while($row = mysql_fetch_assoc($res)) {
  			$data[] = $row;
  		}
  		return $data;
  
  	} else {
  		// INSERT, DELETE, etc...
  		$res = mysql_query($query, $this->conn);
  		if($this->isError($res)) {
  			trigger_error(sprintf("Error executing 'query' with:\n%s\n\nReason: %s\n\n", $query, mysql_error($this->conn)));
  		}
  
  		if ($query_type == SQL_INSERT) {
  			return mysql_insert_id($this->conn);
  		}
  
  		return true;
  	}
  }

  protected function inTransaction() {
	  return $this->in_trx;
  }
  
  protected function beginTransaction() {
    mysql_query("BEGIN", $this->conn);
    $this->in_trx = true;
  }
  
  protected function commit() {
    if($this->in_trx === true) {
     mysql_query("COMMIT", $this->conn);
     $this->in_trx = false; 
    }
  }
  
  protected function rollback() {
    if($this->in_trx === true) {
     mysql_query("ROLLBACK", $this->conn);
     $this->in_trx = false; 
    }    
  }
	
	
}//class

?>
