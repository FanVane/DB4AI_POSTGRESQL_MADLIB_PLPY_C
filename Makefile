MODULES = db4ai_funcs # Ҫ�����Ĺ������б�

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)