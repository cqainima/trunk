<?php
/**
 * Elasticsearch Library
 */
class ElasticSearch
{
    public $index;

    /**
     * constructor setting the config variables for server ip and index.
     */

    public function __construct()
    {
        $ci = &get_instance();
        $ci -> config -> load("elasticsearch");
        $this -> server = $ci -> config -> item('es_server');
        $this -> index = $ci -> config -> item('index');
    }
    /**
     * Handling the call for every function with curl
     * 
     * @param type $path
     * @param type $method
     * @param type $data
     * 
     * @return type
     * @throws Exception
     */

    private function call($path, $method = 'GET', $data = null)
    {
        if (!$this -> index) {
            throw new Exception('$this->index needs a value');
        }
        $url = $this -> server . '/' . $this -> index . '/' . $path;
        $headers = array('Accept: application/json', 'Content-Type: application/json', );
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $url);
        curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, false);
        curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
        switch($method) {
            case 'GET' :
                break;
            case 'POST' :
                curl_setopt($ch, CURLOPT_POST, true);
                curl_setopt($ch, CURLOPT_POSTFIELDS, $data);
                break;
            case 'PUT' :
                curl_setopt($ch, CURLOPT_CUSTOMREQUEST, 'PUT');
                curl_setopt($ch, CURLOPT_POSTFIELDS, json_encode($data));
                break;
            case 'DELETE' :
                curl_setopt($ch, CURLOPT_CUSTOMREQUEST, 'DELETE');
                break;
                break;
        }
        $response = curl_exec($ch);
        $code = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        curl_close($ch);
        return json_decode($response, true);
    }

    /**
     *
     * dls curl
     * @param type $path  url
     * @param type $data  json 数据
     * @return array
     * @throws Exception
     */
    public function dls_call($path, $data = null){
        if (!$this -> index) {
            throw new Exception('$this->index needs a value');
        }
        $url = $this -> server . '/' . $this -> index . '/' . $path;
        $header = array(
            "content-type: application/x-www-form-urlencoded; charset=UTF-8"
        );
        $curl = curl_init();
        curl_setopt($curl, CURLOPT_URL, $url);
        curl_setopt($curl,CURLOPT_HTTPHEADER, $header);
        curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($curl, CURLOPT_POSTFIELDS, $data);
        $response = curl_exec($curl);
        curl_close($curl);
        return json_decode($response, true);
    }

    /**
     *  添加数据
     * @param type $type  表名
     * @param type $id    索引ID
     * @param type $data  要添加的数据 array()
     * 
     * @return id 
     */

    public function add($type, $id, $data)
    {
        if(!empty($type) || !empty($id) || !empty($data) ) {
            $result = $this->call($type . '/' . $id . '/_create', 'PUT', $data);
            if (!$result['_id']) {
                return $result;
            } else {
                return $result['_id'];
            }
        }
        return false;
    }


    /**
     *  更新数据
     * @param type $type  表名
     * @param type $id    索引ID
     * @param type $data  要更新的数据 array()
     * @param type $version 唯一版本号
     * @return id
     */

    public function update($type, $id, $data,$version)
    {
        if(empty($type) || empty($id) || empty($data) || empty($version)){
            return false;
        }else{
            $result =  $this -> call($type . '/' . $id."?version=$version", 'PUT', $data);
            if(!$result['_id']){
                return $result;
            }else{
                return $result['_id'];
            }
        }
    }

    /**
     * 删除数据
     * 
     * @param type $type 表名
     * @param type $id  id
     * @param type $version 唯一版本号
     * @return type 
     */

    public function delete($type, $id,$version)
    {
        if(!empty($type) || !empty($id) || !empty($version) ) {
            $result = $this->call($type . '/' . $id . "?version=$version", 'DELETE');
            if (!$result['_id']) {
                return $result;
            } else {
                return $result['_id'];
            }
        }
        return false;
    }


    public function delete_index(){
            $result = $this->call(null,'DELETE');
            return $result;
    }

    /**
     * 根据type 查询多条数据
     * 
     * @param type $type 表名
     * @param type $q  查询条件
     * @param type $size  查询的条数
     * @param type $from  从第几条开始
     * @return array()
     */

    public function query($type, $q = null,$size = 999,$from=0,$sort="")
    {
        if(!empty($type)){
            if(!empty($sort)){
                $sort_info = explode(" ",$sort);
                //排序
                $result = $this -> call($type . '/_search?sort='.$sort_info[0].':'.$sort_info[1].'&' . http_build_query(array('q' => $q,'size'=>$size,'from'=>$from)));
            }else{
                $result = $this -> call($type . '/_search?' . http_build_query(array('q' => $q,'size'=>$size,'from'=>$from)));
            }
        if(!empty($this->new_array($result))){
            return $this->new_array($result);
            }
        }
        return false;
    }



    /**
     * 使用DSL语句查询
     * @param type $type 表名
     * @return array()
     */
    public function dls_query($type,$query){
        if(!empty($type) || !empty($query)){
            $result = $this -> dls_call($type . '/_search',json_encode($query));
            if(!empty($this->new_array($result))){
                return $this->new_array($result);
            }
        }
        return false;
    }

    /**
     * make a advanced search query with json data to send
     * 
     * @param type $type
     * @param type $query
     * 
     * @return type
     */

    public function advancedquery($type, $query)
    {
        return $this -> call($type . '/_search', 'POST', $query);
    }


    /**
     * 根据索引ID 获取单条数据
     * 
     * @param string  $type The index type
     * @param integer $id   the indentifier for a index
     * @param $field 指定字段  username,age
     * @return array()
     */

    public function get($type, $id,$field ='',$version =0)
    {
        if(!empty($field)){
            $result = $this -> call($type . '/' . $id."?_source=".$field, 'GET');
        }else{
            $result = $this -> call($type . '/' . $id, 'GET');
        }
        if($version == 0){
            return $result['_source'];
        }else{
            return $result;
        }
    }

    /**
     * make a suggest query based on similar looking terms
     * 
     * @param type $query
     * 
     * @return array
     */
    public function suggest($query)
    {
        return $this -> call('_suggest', 'POST', $query);
    }

    /**
     *  局部添加字段
     * @param type $type 表名
     * @param type $query  array()
     * @return array
     */
    public function add_field($type,$id,$query){
        if(!empty($type) || !empty($id) || !empty($query) ){
            $result = $this -> call($type . '/' . $id."/_update", 'POST',json_encode($query));
            return $result;
        }else{
            return false;
        }
    }


    /**
     * create a index with mapping or not
     *
     * @param json $map
     */

    public function create($map = false)
    {
        if (!$map) {
            $this -> call(null, 'PUT');
        } else {
            $this -> call(null, 'PUT', $map);
        }
    }

    /**
     * get status
     *
     * @return array
     */

    public function status()
    {
        return $this -> call('_status');
    }

    /**
     * set the mapping for the index
     *
     * @param string $type
     * @param json   $data
     *
     * @return array
     */

    public function map($type, $data)
    {
        return $this -> call($type . '/_mapping', 'PUT', $data);
    }

    /**
     * get similar indexes for one index specified by id - send data to add filters or more
     *
     * @param string  $type
     * @param integer $id
     * @param string  $fields
     * @param string  $data
     *
     * @return array
     */

    public function morelikethis($type, $id, $fields = false, $data = false)
    {
        if ($data != false && !$fields) {
            return $this -> call($type . '/' . $id . '/_mlt', 'GET', $data);
        } else if ($data != false && $fields != false) {
            return $this -> call($type . '/' . $id . '/_mlt?' . $fields, 'POST', $data);
        } else if (!$fields) {
            return $this -> call($type . '/' . $id . '/_mlt');
        } else {
            return $this -> call($type . '/' . $id . '/_mlt?' . $fields);
        }
    }


    private function new_array($result){
        $new_result = array();
        if($result['hits']['hits']) {
            foreach ($result['hits']['hits'] as $key => $item) {
                $item['_source'] = array_merge(array("id"=>$result['hits']['hits'][$key]['_id']),$item['_source']);
                $new_result[] = $item['_source'];
            }
            return $new_result;
        }
    }
}
