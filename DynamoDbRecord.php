<?php
/*
 * @see: http://docs.aws.amazon.com/aws-sdk-php/v2/api/class-Aws.DynamoDb.DynamoDbClient.html
 */

use Aws\DynamoDb\Enum\Type;
use Aws\DynamoDb\Enum\ComparisonOperator;

require_once("DynamoDb.php");

class DynamodbRecord extends CModel {

    private static $_models = [];

    private $items     = [];
    private $db        = [];
    private $marshaler = [];

    public function __construct() {
        $dynamoDb = new DynamoDb();
        $this->db        = $dynamoDb->getConnectionDb();
        $this->marshaler = $dynamoDb->getMarshaler();
    }

    public function attributeNames() {
        return [];
    }

    public function __get($name){
        $attrs = $this->attributesDefinition();
        if (isset($attrs['pk']) && $attrs['pk']['name'] == $name) {
            if (isset($this->items[$name])) {
                return $this->items[$name];
            }
        }
        return (isset($this->items[$name])) ? $this->items[$name] : null;
    }

    public function __set($name, $value){
        $attrs = $this->attributesDefinition();

        if (!empty($attrs['items']) && in_array($name, $attrs['items'])) {
            $this->items[$name]=$value;
        } else {
            parent::__set($name,$value);
        }
    }

    public static function model($className=__CLASS__){
        if(isset(self::$_models[$className])) {
            return self::$_models[$className];
        }else{
            $model = self::$_models[$className] = new $className(null);
            $model->attachBehaviors($model->behaviors());
            return $model;
        }
    }

    public function findByPk($pk, $ComparisonOperator = 'EQ') {
        $pkName = $this->getPrimaryKeyName();

        $type = is_numeric($pk) ? 'N' : 'S';

        $table = [
            'TableName'     => $this->tableName(),
            'KeyConditions' => [
                $pkName => [
                    'AttributeValueList' => [
                        [$type => $pk]
                    ],
                    'ComparisonOperator' => $ComparisonOperator
                ]
            ]
        ];

        $records = $this->populateRecords($this->db->getIterator('Query',$table));

        return !empty($records) ? $records[0] : null;
    }

    protected function populateRecords($items) {
        $records = [];
        foreach($items as $item){
            $records[]=$this->instantiate($item);
        }
        return $records;
    }

    protected function instantiate($item){
        $class=get_class($this);
        $model=new $class(null);

        foreach ($item as $key => $value) {
            $model->$key = current($value);
        }

        $this->afterFind();
        return $model;
    }

    public function getAttributes($names = NULL) {
        return $this->items;
    }

    public function setAttributes($values, $safeOnly=true) {
        $this->items = $values;
    }

    public function getPrimaryKey() {
        $pkName = $this->getPrimaryKeyName();
        return (is_numeric($this->items[$pkName])) ?
               (int) $this->items[$pkName] : $this->items[$pkName];
    }

    public function getPrimaryKeyName() {
        return $this->attributesDefinition()['pk']['name'];
    }

    public function onBeforeSave($event){
        $this->raiseEvent('onBeforeSave',$event);
    }

    public function onAfterSave($event){
        $this->raiseEvent('onAfterSave',$event);
    }

    public function onBeforeDelete($event){
        $this->raiseEvent('onBeforeDelete',$event);
    }

    public function onAfterDelete($event){
        $this->raiseEvent('onAfterDelete',$event);
    }

    public function onAfterConstruct($event){
        $this->raiseEvent('onAfterConstruct',$event);
    }

    public function onBeforeFind($event){
        $this->raiseEvent('onBeforeFind',$event);
    }

    public function onAfterFind($event){
        $this->raiseEvent('onAfterFind',$event);
    }

    protected function beforeSave(){
        if($this->hasEventHandler('onBeforeSave')){
            $event = new CModelEvent($this);
            $this->onBeforeSave($event);
            return $event->isValid;
        }
        return true;
    }

    protected function afterSave(){
        if($this->hasEventHandler('onAfterSave'))
            $this->onAfterSave(new CEvent($this));
    }

    protected function beforeDelete(){
        if($this->hasEventHandler('onBeforeDelete')){
            $event = new CModelEvent($this);
            $this->onBeforeDelete($event);
            return $event->isValid;
        }
        return true;
    }

    protected function afterDelete(){
        if($this->hasEventHandler('onAfterDelete'))
            $this->onAfterDelete(new CEvent($this));
    }

    protected function afterConstruct(){
        if($this->hasEventHandler('onAfterConstruct'))
            $this->onAfterConstruct(new CEvent($this));
    }

    protected function beforeFind(){
        if($this->hasEventHandler('onBeforeFind'))
            $this->onBeforeFind(new CEvent($this));
    }

    protected function afterFind(){
        if($this->hasEventHandler('onAfterFind'))
            $this->onAfterFind(new CEvent($this));
    }

    private function getIdUser(){
        return Yii::app()->user->id;
    }

    private function getDataAtual(){
        return Yii::app()->dateServer->getToday(true)->format('Y-m-d H:i:s');
    }

    public function generateUuid(){
        return md5(uniqid(rand(), true));
    }

    public function prepareParamsPutItem(){
        return [
            'TableName' => $this->tableName(),
            'Item' => $this->marshaler->marshalJson(
                        (json_encode($this->prepareItemsCasting()))
                      ),
            'ReturnConsumedCapacity' => 'TOTAL'
        ];
    }

    public function prepareParamsUpdateItem(){
        $ExpressionAttributeNames  = [];
        $ExpressionAttributeValues = [];
        $UpdateExpression          = [];

        foreach($this->items as $key=>$value){
            if (is_null($value)){
                continue;
            }

            if ($key != $this->getPrimaryKeyName()){
                $ExpressionAttributeNames["#".$key] = $key;
                $ExpressionAttributeValues[':'.$key] = (is_numeric($value)) ?
                                                        (int) $value : $value;
                $UpdateExpression[]    = "#".$key." = :".$key;
            }
        }

        return  [
            'TableName' => $this->tableName(),
            'Key' => $this->marshaler->marshalJson(
                      json_encode([
                        $this->getPrimaryKeyName() => $this->getPrimaryKey()
                     ])),
            'ExpressionAttributeNames' => $ExpressionAttributeNames,
            'ExpressionAttributeValues' =>  $this->marshaler->marshalJson(
                                  json_encode($ExpressionAttributeValues)),
            'UpdateExpression' => "set ".implode(', ',$UpdateExpression),
            'ReturnValues' => 'ALL_NEW'
        ];
    }

    public function prepareParamsDelete(){
        return [
            'TableName' => $this->tableName(),
            'Key'       => $this->marshaler->marshalJson(
                        json_encode([
                          $this->getPrimaryKeyName() => $this->getPrimaryKey()
                        ])),
            'ReturnValues' => 'ALL_OLD'
        ];
    }

    public function prepareItemsCasting($items = []){
        if (empty($items)){
            $items = $this->items;
        }

        $_items = [];
        foreach ($items as $key=>$item){
            if (is_null($item)){
                continue;
            }
            $_items[$key] = (is_numeric($item)) ? (int) $item : $item;
        }

        return $_items;
    }


    public function save($newItem = false) {
        $attrs = $this->attributesDefinition();

        if (isset($attrs['pk'])) {
            if (!empty($this->items[$attrs['pk']['name']]) && !$newItem){
                $newItem = is_null($this->findByPk($this->items[$attrs['pk']['name']])) ? true : false;
            }else{
                if ($attrs['pk']['auto_increment'] && !isset($this->items[$attrs['pk']['name']])){
                    $this->items[$attrs['pk']['name']] = $this->generateUuid();
                    $newItem = true;
                }
            }
        }

        return ($newItem) ?
            $this->db->putItem($this->prepareParamsPutItem()) :
            $this->db->updateItem($this->prepareParamsUpdateItem());
    }

    public function delete(){
        return $this->db->deleteItem($this->prepareParamsDelete());
    }

    public function getItems($itemsProjection = []){
        if (empty($itemsProjection)){
            $itemsProjection = $this->attributesDefinition()['items'];
        }

        $itemsTable = $this->db->getItem ([
            'TableName' => $this->tableName(),
            'ConsistentRead' => true,
            'Key' => $this->marshaler->marshalJson(
                      json_encode(
                      [$this->getPrimaryKeyName() => $this->getPrimaryKey()
                    ])),
            'ProjectionExpression' => implode(',',$itemsProjection)
        ]);

        $itemsTable = $this->populateRecords($itemsTable);

        return $itemsTable[0];
    }

    public function findItems($params = []){
        $response = $this->db->query($params);
        $items = [];
        foreach ($response['Items'] as $item){
            $items[] = $item;
        }
        return $this->populateRecords($items);
    }

}
