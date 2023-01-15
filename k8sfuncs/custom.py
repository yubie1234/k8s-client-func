from multiprocessing import Manager
import kubernetes
from kubernetes import client, config
import base64
import time
import os 

# kuberentes python client 12.0.1
# kubernetes API version 1.17.4

KUBE_SHARE_DICT = Manager().dict()
KUBER_CONFIG_PATH = "/root/.kube/config"

# def update_kube_config():
#     with jf_plock:
#         global coreV1Api
#         global extensV1Api
#         global apiextensV1Api
#         try:
#             # ** version 이슈 있음 **
#             # kubernetes 12.0.1 기준 api instance.
#             # config.load_kube_config(config_file=KUBER_CONFIG_PATH, persist_config=False)
#             config.load_kube_config(config_file=KUBER_CONFIG_PATH)
#             coreV1Api = kubernetes.client.CoreV1Api()
#             extensV1Api = kubernetes.client.ExtensionsV1beta1Api()
#             apiextensV1Api = kubernetes.client.ApiextensionsV1Api() # For SystemCheck
#         except Exception as e:
#             print("Config Load Unknown Error" ,e)

# update_kube_config()


POD_LIST_KEY = "pod_list"
NODE_LIST_KEY = "node_list"
SERVICE_LIST_KEY = "service_list"
INGRESS_SERVICE_LIST_KEY = "ingress_service_list"
INGRESS_LIST_KEY = "ingress_list"

class KubeShareFunc():
    """
        조회용 API 중 지정한 API 등록 및 serialize, deserialize 정의할 수 있는 Object

    """
    def __init__(self):
        # response_type update by _response_type_parser
        self._kuber_api_init()
        self.main_func_dict = {
            POD_LIST_KEY: {
                "func": self.coreV1Api.list_namespaced_pod, 
                "kwargs": {"namespace": "default"}, # API 함수에서 요구하는 파라미터들 지정
                "response_type": None # V1PodList (_response_type_parser에서 자동으로 지정) deserialize 시 필요함
            },
            NODE_LIST_KEY: {
                "func": self.coreV1Api.list_node, 
                "kwargs": {},
                "response_type": None
            },
            SERVICE_LIST_KEY: {
                "func": self.coreV1Api.list_namespaced_service,  
                "kwargs": {"namespace": "default"},
                "response_type": None
            },
            INGRESS_SERVICE_LIST_KEY: {
                "func": self.coreV1Api.list_namespaced_service,
                "kwargs": {"namespace": "ingress-nginx"},
                "response_type": None
            },
            INGRESS_LIST_KEY: {
                "func": self.extensV1Api.list_namespaced_ingress,
                "kwargs": {"namespace": "default"},
                "response_type": None
            }
        }
        
        self._main_func_init()

    def _kuber_api_init(self):
        """
            Main Func에서 사용할 Api들 미리 정의해두는 곳
            사용하고자 하는 Api가 더 있으면 추가하여 사용
        """
        config.load_kube_config(config_file=KUBER_CONFIG_PATH)
        self.coreV1Api = kubernetes.client.CoreV1Api()
        self.extensV1Api = kubernetes.client.ExtensionsV1beta1Api()
        self.apiextensV1Api = kubernetes.client.ApiextensionsV1Api() # For SystemCheck

    def _main_func_init(self):
        for func_key in self.get_func_key_list():
            self._response_type_parser(func_key=func_key)
            self._serialize_form_and_self_deserialize_test(func_key=func_key)

    def _response_type_parser(self, func_key):
        func_info = self.get_func_info(func_key=func_key)
        func_info["response_type"] = type(func_info["func"](**func_info["kwargs"])).__name__
        
        
    def _serialize_form_and_self_deserialize_test(self, func_key):  
        func_info = self.get_func_info(func_key=func_key)
        # auto deserialize form
        item_list = func_info["func"](**func_info["kwargs"])

        # serialize form
        item_list_se = func_info["func"](**func_info["kwargs"], _preload_content=False).data

        # self deserialize form
        item_list_de = self._deserialize(item_list_se, func_info["response_type"])
        
        assert type(item_list) is type(item_list_de), "{} | {}".format(type(item_list), type(item_list_de))

        item_list_to_dict = item_list.to_dict()
        item_list_de_to_dict = item_list_de.to_dict()

        key_list = item_list_de_to_dict.keys()

        assert item_list_to_dict.keys() == item_list_de_to_dict.keys(), "{} | {}".format(item_list_to_dict.keys(),item_list_de_to_dict.keys())

        # metadadata의 경우 조회 시차 때문에 resource_version이 다르게 나올 수 있음
        # 모든 조회 결과가 다르게 나온다면 확인이 필요
        for key in key_list:
            if item_list_to_dict[key] != item_list_de_to_dict[key]:
                print("[Warn] {} - {} is not Same.".format(func_key ,key))
    
    def _wrap_fake_response(self, serialize_item):
        class FakeResponse:
            def __init__(self, serialize_item):
                self.data = serialize_item
        return FakeResponse(serialize_item)
    
    def _deserialize(self, serialize_item, reponse_type):
        if type(serialize_item).__name__ == "HTTPResponse":
            pass
        if type(serialize_item).__name__ == "bytes":
            serialize_item = self._wrap_fake_response(serialize_item)
            
        return kubernetes.client.ApiClient().deserialize(serialize_item, reponse_type)
    
    def get_func_key_list(self):
        """
        return (list) : key name list.
        """
        return self.main_func_dict.keys()
    
    def get_func_info(self, func_key):
        return self.main_func_dict[func_key]
    
    def convert_serialize_item_to_deserialize_item(self, serialize_item, func_key):
        func_info = self.get_func_info(func_key=func_key)
            
        return self._deserialize(serialize_item, func_info["response_type"])
    
    def get_deserialize_item_list(self, func_key, **kwargs):
        """
        func_key (str) : return of get_func_key_list()
        """
        # Object
        func_info = self.get_func_info(func_key=func_key)
        func_info["kwargs"].update(kwargs)
        return func_info["func"](**func_info["kwargs"])
        
    def get_serialize_item_list(self, func_key, **kwargs):
        """
        func_key (str) : return of get_func_key_list()
        """
        # 공유용
        func_info = self.get_func_info(func_key=func_key)
        func_info["kwargs"].update(kwargs)
        return func_info["func"](**func_info["kwargs"], _preload_content=False)
    
    #################################################################################
    
    # POD LIST
    def get_deserialize_pod_list(self, **kwargs):
        """
        return pod list kubernetes object
        """ 
        return self.get_deserialize_item_list(func_key=POD_LIST_KEY, **kwargs)
    
    def get_serialize_pod_list(self, **kwargs):
        """
        return pod list response (byte)
        """
        return self.get_serialize_item_list(func_key=POD_LIST_KEY, **kwargs)

    # NODE LIST
    def get_deserialize_node_list(self, **kwargs):
        """
        return node list kubernetes object
        """ 
        return self.get_deserialize_item_list(func_key=NODE_LIST_KEY, **kwargs)
    
    def get_serialize_node_list(self, **kwargs):
        """
        return node list response (byte)
        """
        return self.get_serialize_item_list(func_key=NODE_LIST_KEY, **kwargs)
    
    # SERVICE LIST
    def get_deserialize_service_list(self, **kwargs):
        """
        return service list kubernetes object
        """ 
        return self.get_deserialize_item_list(func_key=SERVICE_LIST_KEY, **kwargs)
    
    def get_serialize_service_list(self, **kwargs):
        """
        return service list response (byte)
        """
        return self.get_serialize_item_list(func_key=SERVICE_LIST_KEY, **kwargs)
    
    # INGRESS SERVICE LIST
    def get_deserialize_ingress_service_list(self, **kwargs):
        """
        return ingress service list kubernetes object
        """ 
        return self.get_deserialize_item_list(func_key=INGRESS_SERVICE_LIST_KEY, **kwargs)
    
    def get_serialize_ingress_service_list(self, **kwargs):
        """
        return ingress service list response (byte)
        """
        return self.get_serialize_item_list(func_key=INGRESS_SERVICE_LIST_KEY, **kwargs)
    
    # INGRESS LIST
    def get_deserialize_ingress_list(self, **kwargs):
        """
        return ingress list kubernetes object
        """ 
        return self.get_deserialize_item_list(func_key=INGRESS_LIST_KEY, **kwargs)
    
    def get_serialize_ingress_list(self, **kwargs):
        """
        return ingress list response (byte)
        """
        return self.get_serialize_item_list(func_key=INGRESS_LIST_KEY, **kwargs)  
    

kube_share_func = KubeShareFunc()

# 공유 데이터
class KubeData():
    def __init__(self, kube_share_func, kube_share_dict, namespace="default"):
        """
        kube_share_func (KubeShareFunc)
        kube_share_dict (dict or multiprocessing.managers.DictProxy) 
        """
        self.kube_share_func = kube_share_func
        self.kube_share_dict = kube_share_dict
        self._init_kube_share_dict()
        
        self.master_pid = None
        self.node_update_func = []
        self.apiserver_addr = self._get_apiserver_addr()
        self.token = self._get_token()
        
        self.pod_list = None
        self.node_list = None
        self.service_list = None
        self.ingress_service_list = None
        self.ingress_list = None
        self.update_all_list()
        # print(self.kube_share_dict.keys())
        # print(kube_share_dict.keys())


    def _init_kube_share_dict(self):
        kube_share_func = self.kube_share_func
        kube_share_dict = self.kube_share_dict
        for func_key in kube_share_func.get_func_key_list():
            if kube_share_dict.get(func_key) is None:
                kube_share_dict[func_key] = kube_share_func.get_serialize_pod_list().data

    def set_master_pid(self, pid):
        self.master_pid = pid

    def is_master(self):
        return self.master_pid == os.getpid()
    
    def set_update_node_labels_func(self, update_func):
        if type(update_func) == type([]):
            self.node_update_func += update_func
        else :
            self.node_update_func.append(update_func)

    def _run_update_node_labels_func(self):
        for f in self.node_update_func:
            f()

    def _get_apiserver_addr(self):
        """
        kube config file 읽어서 apiserver_addr parsing
        """
        with open(KUBER_CONFIG_PATH, "r") as fr:
            for line in fr.readlines():
                if "server:" in line:
                    line = line.replace("server:","").replace(" ","").replace("\n","")
                    return line
    
    def get_apiserver_addr(self):
        return self.apiserver_addr

    def _get_token(self):
        """
        python kubernetes api에서 제공해주지 않는 기능에 대해 직접 api call을 요청할 때 필요한 token parsing
        """
        def get_service_account_default_secrets_name():
            for service_account in kube_share_func.coreV1Api.list_service_account_for_all_namespaces().items:
                if service_account.metadata.name == "default":
                    return service_account.secrets[0].name

        secret_list = kube_share_func.coreV1Api.list_secret_for_all_namespaces()
        for secret in secret_list.items:
            if secret.metadata.name == get_service_account_default_secrets_name():
                return base64.b64decode(secret.data["token"]).decode()
    
    def get_token(self):
        return self.token
    
    def _convert_serialize_item_to_deserialize_item(self, func_key):
        """
        serialize item -> deserialize item
        multiprocessing manager로 부터 공유받는 serialize item 을 object로 변경
        """
        return self.kube_share_func.convert_serialize_item_to_deserialize_item(self.kube_share_dict.get(func_key), func_key)
           
    def update_all_list(self, namespace="default", force=False):
        try:
            pod_list_resource_version = self.update_pod_list(namespace=namespace)
            service_list_resource_version = self.update_service_list(namespace=namespace)
            ingress_service_list_resource_version = self.update_ingress_service_list(namespace="ingress-nginx")
            node_list_resource_version = self.update_node_list()
            ingress_list_resource_version = self.update_ingress_list(namespace=namespace)

            self.kube_share_dict["resource_version"] = {
                POD_LIST_KEY: pod_list_resource_version,
                SERVICE_LIST_KEY: service_list_resource_version,
                INGRESS_SERVICE_LIST_KEY: ingress_service_list_resource_version,
                NODE_LIST_KEY: node_list_resource_version,
                INGRESS_LIST_KEY: ingress_list_resource_version
            }
        except Exception as e:
            print("Kubernetes Update all Unknown error", e)
            # update_kube_config()
        
    def update_node_list(self):
        old_node_list = self._convert_serialize_item_to_deserialize_item(NODE_LIST_KEY)
        
        resource_version = self._update_node_list()
        
        new_node_list = self._convert_serialize_item_to_deserialize_item(NODE_LIST_KEY)
        
        if self.is_master():
            if len(old_node_list.items) != len(new_node_list.items):
                self._run_update_node_labels_func()

        return resource_version

    def _update_node_list(self, **kwargs):
        self.kube_share_dict[NODE_LIST_KEY] = self.kube_share_func.get_serialize_node_list(**kwargs).data
        return self._get_list_resource_version(list_key=NODE_LIST_KEY)

    def update_pod_list(self, **kwargs):
        self.kube_share_dict[POD_LIST_KEY] = self.kube_share_func.get_serialize_pod_list(**kwargs).data
        return self._get_list_resource_version(list_key=POD_LIST_KEY)
    
    def update_service_list(self, **kwargs):
        self.kube_share_dict[SERVICE_LIST_KEY] = self.kube_share_func.get_serialize_service_list(**kwargs).data
        return self._get_list_resource_version(list_key=SERVICE_LIST_KEY)

    def update_ingress_service_list(self, **kwargs):
        self.kube_share_dict[INGRESS_SERVICE_LIST_KEY] = self.kube_share_func.get_serialize_ingress_service_list(**kwargs).data
        return self._get_list_resource_version(list_key=INGRESS_SERVICE_LIST_KEY)

    def update_ingress_list(self, **kwargs):
        self.kube_share_dict[INGRESS_LIST_KEY] = self.kube_share_func.get_serialize_ingress_list(**kwargs).data
        return self._get_list_resource_version(list_key=INGRESS_LIST_KEY)

    def _get_list_resource_version(self, list_key):
        st = time.time()
        item_list = self._convert_serialize_item_to_deserialize_item(list_key)
        resource_version = item_list.metadata.resource_version
        return resource_version

    def _check_list_resource_version(self, item_list, list_key):
        if item_list is None:
            return False
        resource_version = item_list.metadata.resource_version
        return self.kube_share_dict["resource_version"][list_key] == resource_version


    def get_pod_list(self, try_update=False, namespace="default"):
        if try_update:
            self.update_pod_list(namespace=namespace)

        if not self._check_list_resource_version(item_list=self.pod_list, list_key=POD_LIST_KEY):
            # print("POD LIST UPDATE !!")
            pod_list = self._convert_serialize_item_to_deserialize_item(POD_LIST_KEY)
            self.pod_list = pod_list
        else :
            # print("POD LIST MEMORY USE !!")
            pass
            
        # self._get_list_resource_version(item_list=self.pod_list)
        return self.pod_list

    def get_service_list(self, try_update=False, namespace="default"):
        if try_update:
            self.update_service_list(namespace=namespace)
        
        if not self._check_list_resource_version(item_list=self.service_list, list_key=SERVICE_LIST_KEY):
            # print("SERVICE LIST UPDATE !!")
            service_list = self._convert_serialize_item_to_deserialize_item(SERVICE_LIST_KEY)
            self.service_list = service_list
        else:
            # print("SERVICE LIST MEMORY USE !!")
            pass
        
        # service_list = self._convert_serialize_item_to_deserialize_item(SERVICE_LIST_KEY)            
        return self.service_list

    def get_ingress_service_list(self, try_update=False):
        if try_update:
            self.update_ingress_service_list(namespace="ingress-nginx")

        if not self._check_list_resource_version(item_list=self.ingress_service_list, list_key=INGRESS_SERVICE_LIST_KEY):
            # print("INGRESS SERVICE LIST UPDATE !!")
            ingress_service_list = self._convert_serialize_item_to_deserialize_item(INGRESS_SERVICE_LIST_KEY)
            self.ingress_service_list = ingress_service_list
        else:
            # print("INGRESS SERVICE LIST MEMORY USE !!")
            pass

        # ingress_service_list = self._convert_serialize_item_to_deserialize_item(INGRESS_SERVICE_LIST_KEY)    
        return self.ingress_service_list

    def get_node_list(self, try_update=False):
        if try_update:
            self.update_node_list()

        if not self._check_list_resource_version(item_list=self.node_list, list_key=NODE_LIST_KEY):
            # print("NODE LIST UPDATE !!")
            node_list = self._convert_serialize_item_to_deserialize_item(NODE_LIST_KEY)
            self.node_list = node_list
        else:
            # print("NODE LIST MEMORY USE !!")
            pass

        # node_list = self._convert_serialize_item_to_deserialize_item(NODE_LIST_KEY)
        return self.node_list

    def get_ingress_list(self, try_update=False, namespace="default"):
        if try_update:
            self.update_ingress_list(namespace=namespace)

        if not self._check_list_resource_version(item_list=self.ingress_list, list_key=INGRESS_LIST_KEY):
            # print("INGRESS LIST UPDATE !!")
            ingress_list = self._convert_serialize_item_to_deserialize_item(INGRESS_LIST_KEY)
            self.ingress_list = ingress_list
        else:
            # print("INGRESS LIST MEMORY USE !!")
            pass

        # ingress_list = self._convert_serialize_item_to_deserialize_item(INGRESS_LIST_KEY)
        return self.ingress_list

kube_data = KubeData(kube_share_func=kube_share_func, kube_share_dict=KUBE_SHARE_DICT)
