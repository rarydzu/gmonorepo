package kvstore

type Store interface {
	Get(key string) (string, error)
	Set(key string, value string) error
	Delete(key string) error
	Flush() (string, error)
	Send(storeVersion string) error
	Retrieve(storeVersion string) error
}

type KVStore struct {
	Store
}

func NewKVStore(store Store) *KVStore {
	return &KVStore{store}
}

func (kv *KVStore) Get(key string) (string, error) {
	return kv.Store.Get(key)
}

func (kv *KVStore) Set(key string, value string) error {
	return kv.Store.Set(key, value)
}
