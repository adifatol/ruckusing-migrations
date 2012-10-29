<?php

class Ruckusing_TableDefinition {

	private $columns = array();
	private $adapter;
	
	function __construct($adapter) {
		$this->adapter = $adapter;
	}
	
	/*
	public function column($name, $type, $options = array()) {
	    die;
		$column = new Ruckusing_ColumnDefinition($this->adapter, $name, $type);
		$native_types = $this->adapter->native_database_types();
		echo "\n\nCOLUMN: " . print_r($options,true) . "\n\n";
		
		if($native_types && array_key_exists('limit', $native_types) && !array_key_exists('limit', $options)) {
			$limit = $native_types['limit'];
		} elseif(array_key_exists('limit', $options)) {
			$limit = $options['limit'];
		} else {
			$limit = null;
		}		
		$column->limit = $limit;
		
		if(array_key_exists('precision', $options)) {
			$precision = $options['precision'];
		} else {
			$precision = null;
		}
		$column->precision = $precision;

		if(array_key_exists('scale', $options)) {
			$scale = $options['scale'];
		} else {
			$scale = null;
		}
		$column->scale = $scale;

		if(array_key_exists('default', $options)) {
			$default = $options['default'];
		} else {
			$default = null;
		}
		$column->default = $default;

		if(array_key_exists('null', $options)) {
			$null = $options['null'];
		} else {
			$null = null;
		}
		$column->null = $null;

		if($this->included($column) == false) {
			$this->columns[] = $column;
		}		
	}//column
	*/
	
	/*
		Determine whether or not the given column already exists in our 
		table definition.
		
		This method is lax enough that it can take either a string column name
		or a Ruckusing_ColumnDefinition object.
	*/
	public function included($column) {
		$k = count($this->columns);
		for($i = 0; $i < $k; $i++) {
			$col = $this->columns[$i];
			if(is_string($column) && $col->name == $column) {
				return true;
			}
			if(($column instanceof Ruckusing_ColumnDefinition) && $col->name == $column->name) {
				return true;
			}
		}
		return false;
	}	
	
	public function to_sql() {
		return join(",", $this->columns);
	}
}

class Ruckusing_ColumnDefinition {
	private $adapter;
	public $name;
	public $type;
	public $properties;
	private $options = array();
	
	function __construct($adapter, $name, $type, $options = array()) {
		$this->adapter = $adapter;
		$this->name = $name;
		$this->type = $type;
	    $this->options = $options;
	}

	public function to_sql() {
		$column_sql = sprintf("%s %s", $this->adapter->identifier($this->name), $this->sql_type());
		$column_sql .= $this->adapter->add_column_options($this->type, $this->options);			
		return $column_sql;
	}

	public function __toString() {
	  //Dont catch any exceptions here, let them bubble up
	  return $this->to_sql();
	}

	private function sql_type() {
    return $this->adapter->type_to_sql($this->type, $this->options);
	}
}

?>