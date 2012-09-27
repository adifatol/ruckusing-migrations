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


?>