.PHONY: install index run clean

install:
	pip install -r requirements.txt

index:
	python scripts/build_index.py

run:
	streamlit run app.py

clean:
	rm -rf chroma_db/
