<?php


//----------------------------
// DATABASE CONFIGURATION
//----------------------------
$ruckusing_db_config = array(
	
    'development' => array(
        'type'      => 'pgsql',
        'host'      => 'localhost',
        'port'      => 5432,
        'database'  => 'oligopoly_new',
        'user'      => 'postgres',
        'password'  => 'postgres'
    ),

	'test' 				=> array(
			'type' 		=> 'pgsql',
			'host' 		=> 'localhost',
			'port'		=> 5432,
			'database' 	=> 'oligopoly_new',
			'user' 		=> 'postgres',
			'password' 	=> 'postgres'
	)
	
);

if (is_file(RUCKUSING_BASE . '/config/database.over.php')){
	include require RUCKUSING_BASE . '/config/database.over.php';
}

?>