csv_files = $(wildcard data/raw/*.csv)
.PHONY: gen
gen: 
	$(foreach a, $(csv_files), sed -i.bak 1i"$$(cat data/header_dir/$(notdir $(a)))" $(a);)
.PHONY: psql
psql:
	@$(foreach a, $(csv_files), csvsql $(a) --db postgresql://postgres:acris@127.0.0.1/acris --table $(basename $(notdir $(a))) --insert;)
