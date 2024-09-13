import sys

def check_import(module_name, install_alias=None):
    try:
        if install_alias is not None:
            name = install_alias
        else:
            name = module_name
        module = __import__(module_name)
        print(f"{name} - version {module.__dict__.get('__version__', '(not listed)')} installed")
        return module
    except ImportError:
        print(f"***** WARNING ***** {name} is not installed")
        return None

def check_spacy():
    try:
        import spacy
        from spacytextblob.spacytextblob import SpacyTextBlob
        
        nlp = spacy.load("en_core_web_sm")
        nlp.add_pipe("spacytextblob")

        text = "This workshop on Prefect by Adam is going to be awesome!"
        doc = nlp(text)
        
        # Check entities
        try:
            assert "Prefect" in [ent.text for ent in doc.ents], "Entities extraction failed"
            assert "Adam" in [ent.text for ent in doc.ents], "Entities extraction failed"
        except AssertionError as e:
            print(f"***** WARNING ***** {e}")

        # Check polarity
        try:
            assert doc._.blob.polarity == 1, "Text polarity is incorrect"
        except AssertionError as e:
            print(f"***** WARNING ***** {e}")
    except ModuleNotFoundError as e:
        print(f"***** WARNING ***** {e}")
    except ImportError as e:
        print(f"***** WARNING ***** {str(e).split(':')[1].strip()} is not installed")
    except OSError:
        print("***** WARNING ***** Spacy model not installed! Please run: 'python -m spacy download en_core_web_sm'")

def check_mongodb():
    try:
        from dotenv import dotenv_values
        import pymongo
        from pymongo.server_api import ServerApi

        config = dotenv_values(".env")
        client = pymongo.MongoClient(config.get("MONGO_URI"), server_api=ServerApi('1'))
        client.admin.command("ping")
        print("Pinged your deployment. You successfully connected to MongoDB Atlas!")
    except ModuleNotFoundError as e:
        print(f"***** WARNING ***** {e}")    
    except ImportError as e:
        print(f"***** WARNING ***** {str(e).split(':')[1].strip()} is not installed")
    except pymongo.errors.ServerSelectionTimeoutError:
        print("***** WARNING ***** Cannot connect to MongoDB server. Check your MONGO_URI in the .env file.")
    except Exception as e:
        print(f"***** WARNING ***** {e}")

if __name__ == "__main__":
    # Check dependencies
    check_import('prefect')
    check_import('kafka', 'kafka-python-ng')
    check_import('spacy')
    check_import('spacytextblob')
    check_import('pymongo')
    check_import('dotenv', 'python-dotenv')

    # Check Spacy and MongoDB
    check_spacy()
    check_mongodb()
