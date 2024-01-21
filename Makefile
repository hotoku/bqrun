PYPI_TOKEN := "" # 環境変数として設定すること


define release
$1:
	bump2version $$@
	git push --tag
	git push
endef


$(foreach part,patch minor major,$(eval $(call release,$(part))))


.PHONY: build
build:
	poetry build


.PHONY: publish
publish: build
	poetry publish --username=__token__ --password=$(PYPI_TOKEN)
