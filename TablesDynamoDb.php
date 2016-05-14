<?php

class TablesDynamoDb{

    private $db;

    public function setConnectionDbTables($db){
        $this->db = $db;
    }

    public function getDb(){
        return $this->db;
    }

    private function preExistingTable($table){
        $response = $this->db->listTables();
        return in_array($table,$response['TableNames']);
    }

    public function createTableLog() {
        $tableName = 'log';

        if ($this->preExistingTable($tableName)){
            return false;
        }

        $params = [
            'TableName' => $tableName,
            'AttributeDefinitions' => [
                ['AttributeName' => 'idLog'  ,'AttributeType' => 'S'],
                ['AttributeName' => 'idUser' ,'AttributeType' => 'N'],
                ['AttributeName' => 'dateLog','AttributeType' => 'S'],
                ['AttributeName' => 'type'   ,'AttributeType' => 'S'],
            ],
            'KeySchema' => [
                ['AttributeName' => 'idLog','KeyType' => 'HASH']
            ],
            'GlobalSecondaryIndexes' => [
                [
                    'IndexName' => 'idx_idUser',
                    'KeySchema' => [
                        [ 'AttributeName' => 'idUser', 'KeyType' => 'HASH'],
                        [ 'AttributeName' => 'dateLog', 'KeyType' => 'RANGE']
                    ],
                    'Projection' => [ 'ProjectionType' => 'ALL' ],
                    'ProvisionedThroughput' => [
                        'ReadCapacityUnits'    => 1,
                        'WriteCapacityUnits' => 1
                    ]
                ],
               [
                   'IndexName' => 'idx_type',
                   'KeySchema' => [
                       [ 'AttributeName' => 'type', 'KeyType' => 'HASH'],
                       [ 'AttributeName' => 'dateLog', 'KeyType' => 'RANGE']
                   ],
                   'Projection' => [ 'ProjectionType' => 'ALL' ],
                   'ProvisionedThroughput' => [
                       'ReadCapacityUnits'    => 1,
                       'WriteCapacityUnits' => 1
                   ]
               ]
            ],
            'ProvisionedThroughput' => [
                'ReadCapacityUnits'    => 1,
                'WriteCapacityUnits' => 1,
                'NumberOfDecreasesToday' => 0
            ],
            'ItemCount'=> 0,
            'TableSizeBytes'=> 0
        ];

        try {

            $this->getDb()->createTable($params);

            $this->getDb()->waitUntil('TableExists', [
                'TableName' => $params['TableName']
            ]);

            return true;

        } catch (DynamoDbException $e) {
            echo $e->getMessage() . "\n";
            return false;
        }

    }

    public function deleteTableLog(){
        $tableName = 'log';

        if (!$this->preexistingTable($tableName)){
            return false;
        }

        try {

            $this->getDb()->deleteTable([ 'TableName' => $tableName]);

            $this->getDb()->waitUntil('TableNotExists', [
                'TableName' => $tableName,
                '@waiter' => [
                    'delay'       => 5,
                    'maxAttempts' => 20
                ]
            ]);

            return true;

        } catch (DynamoDbException $e) {
            echo $e->getMessage() . "\n";
            return false;
        }
    }
}
