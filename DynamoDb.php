<?php
use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDb\Exception\DynamoDbException;
use Aws\DynamoDb\Marshaler;

require_once 'TablesDynamoDb.php';

class DynamoDb extends TablesDynamoDb{

    private $connection = [];
    private $marshaler  = [];

    public function __construct(){
        $this->setConnectionDb(Yii::app()->params->aws);
        $this->setConnectionDbTables($this->connection);
        $this->setMarshaler();
    }

    public function setConnectionDb($aws){
        $this->connection = DynamoDbClient::factory([
            'credentials' => $aws['dynamodb']['credentials'],
            'region'      => $aws['dynamodb']['region'],
            'version'     => $aws['dynamodb']['version'],
            'endpoint'    => $aws['dynamodb']['endpoint']
        ]);
    }

    public function setMarshaler(){
        $this->marshaler = new Marshaler();
    }

    public function getMarshaler(){
        return $this->marshaler;
    }

    public function getConnectionDb(){
        return $this->connection;
    }

    public function putItem($params = []){
        try {
            $this->connection->putItem($params);
            return true;
        } catch (DynamoDbException $e) {
            echo $e->getMessage() . "\n";
            return false;
        }
    }

    public function updateItem($params = []){
        try {
            $this->connection->updateItem($params);
            return true;
        } catch (DynamoDbException $e) {
            echo $e->getMessage() . "\n";
            return false;
        }
    }

    public function deleteItem($params = []){
        try {
            $this->connection->deleteItem($params);
            return true;
        } catch (DynamoDbException $e) {
            echo $e->getMessage() . "\n";
            return false;
        }
    }

    public function createTables(){
        if ($this->createTableLog()){
            print_r(" >> created log!\n");
        }
    }

    public function dropTables(){
        if ($this->deleteTableLog()){
            print_r(" >> deleted log!\n");
        }
    }

}
