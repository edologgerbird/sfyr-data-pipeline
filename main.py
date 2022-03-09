import pandas as pd
import SGXDataExtractor

if __name__ == '__main__':
    sgx_data_extractor_layer = SGXDataExtractor.SGXDataExtractor()
    sgx_data_extractor_layer.get_SGX_data()
