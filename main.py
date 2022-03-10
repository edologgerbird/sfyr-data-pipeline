import pandas as pd
import SGXDataExtractor

if __name__ == '__main__':
    sgx_data_extractor_layer = SGXDataExtractor.SGXDataExtractor()
    sgx_data_extractor_layer.load_SGX_data_from_source()
