<?php


class Mysql {

    public $Host     	= "";
    public $Database 	= "";
    public $User     	= "";
    public $Password 	= "";
    public $charset  	= "utf8";

    public $Link_ID  	= 0;
    public $Query_ID 	= 0;
    public $Record   	= array();
    public $Row      	= 0;

    public $Errno    	= 0;
    public $Error    	= "";
    public $Auto_Free 	= 1;		//set this to 1 to automatically free results

    public function __construct() {
    }

    public function connect() {
        if (!$this->Link_ID) {
            $this->Link_ID = mysql_connect($this->Host, $this->User, $this->Password, TRUE);
            if (!$this->Link_ID) {
                $this->Errno = 1;
                $this->Error = "DB Connect Error! ".$this->Host;
                return false;
            }

            @mysql_select_db($this->Database, $this->Link_ID);
            mysql_query("SET NAMES 'utf8'", $this->Link_ID);
        }

        return true;
    }

    public function query($Query_String) {
        if ("" == $Query_String) {
            return 0;
        }
        if (!$this->Link_ID) {
            if($this->connect() == false) {
                return false;
            }
        }

        $this->Query_ID = mysql_query($Query_String, $this->Link_ID);
        if (!$this->Query_ID) {
            $this->Errno = 2;
            $this->Error = "DB Query Error! ".$Query_String;
            return false;
        }
        return $this->Query_ID;
    }

    /**
     * 根据结果集返回结果数据
     *
     * @return 0 or array
     */
    public function fetchAll() {
        if (!$this->Query_ID) {
            return 0;
        }
        $this->Record = array();
        while ($row = mysql_fetch_array($this->Query_ID, MYSQL_ASSOC)) {
            $this->Record[] = $row;
        }

        if ($this->Auto_Free) {
            $this->free_result();
        }

        return $this->Record;
    }

    public function fetchOne($sql) {
        $this->query($sql);

        return $this->next_record();
    }

    public function getColumnName($tableName) {
        if (!$tableName) {
            return null;
        }

        $sql = "desc ".$tableName;
        $this->query($sql);
        $data = $this->fetchAll();

        $column = array();
        if (!empty($data) && is_array($data)) {
            foreach ($data as $val) {
                $column[$val["Field"]] = $val["Default"];
            }
        }

        return $column;
    }

    private function next_record() {
        if (!$this->Query_ID) {
            return 0;
        }

        $this->Record = array();
        if ($tmp = mysql_fetch_row($this->Query_ID)) {
            $count = mysql_num_fields($this->Query_ID);
            for ($i=0; $i<$count; $i++) {
                $fieldinfo = mysql_fetch_field($this->Query_ID,$i);
                $this->Record[strtolower($fieldinfo->name)] = $tmp[$i];
            }
            if ($this->Auto_Free) {
                $this->free_result();
            }
        }

        return $this->Record;
    }

    public function seek($pos) {
        mysql_data_seek($this->Query_ID,$pos);
        $this->Row = $pos;
    }

    public function metadata($table) {
        $count = 0;
        $id = 0;
        $res = array();

        $this->connect();
        $id = mysql_query("SELECT * FROM ".$table, $this->Link_ID);
        if (!$id) {
            $this->Errno = 3;
            $this->Error = "DB Error!";
            $this->halt("Metadata query failed.");
        }

        $count = mysql_num_fields($id);
        for ($i=0; $i<$count; $i++) {
            $info = mysql_fetch_field($id, $i);
            $res[$i]["table"] = $table;
            $res[$i]["name"]  = $info["name"];
            $res[$i]["len"]   = $info["max_length"];
            $res[$i]["flags"] = $info["numeric"];
        }
        $this->free_result();

        return $res;
    }

    public function affected_rows() {
        if ($this->Link_ID) {
            return mysql_affected_rows($this->Link_ID);
        }else {
            return 0;
        }
    }

    public function fetch_row() {
        if ($this->Query_ID) {
            return mysql_fetch_row($this->Query_ID);
        }else {
            return 0;
        }
    }

    public function fetch_array() {
        if ($this->Query_ID) {
            return mysql_fetch_array($this->Query_ID);
        }else {
            return 0;
        }
    }

    public function num_rows() {
        if ($this->Query_ID) {
            return mysql_num_rows($this->Query_ID);
        }else {
            return 0;
        }
    }

    public function num_fields() {
        if ($this->Query_ID) {
            return mysql_num_fields($this->Query_ID);
        }else {
            return 0;
        }
    }

    public function close() {
        if ($this->Link_ID) {
            mysql_close($this->Link_ID);
        }
    }

    public function free_result() {
        if ($this->Query_ID) {
            mysql_free_result($this->Query_ID);
            $this->Query_ID = 0;
        }
    }

    private function halt($msg) {
        die("[DB Error]:".$msg);
    }

    function getInsertId(){
        return mysql_insert_id();
    }
}
?>