from unittest import mock

import pytest
import json
from backtest.domains.stockdata import StockData
from backtest.module_compet.pandas import pd
from backtest.repository.webrepo.crypto.bithumb_repo import BithumbRepo


bithumb_30m = '{"status":"0000","data":[[16802568000063000","34.24370087"],[1680260400000,"36992000","36993000","37068000","36974000","22.69408776"],[1680262200000,"36993000","37044000","37090000","36941000","27.23726519"],[1680264000000,"37042000","37116000","37128000","36993000","29.81046921"],[1680265800000,"37110000","37272000","37397000","37055000","91.88140951"],[1680267600000,"37281000","37451000","37504000","37254000","63.40125193"]]}'
bithumb_1m = '{"status":"0000","data":[[1685576700000,"36450000","36450000","36450000","36439000",".2836"],[1685576760000,"36447000","36434000","36452000","36434000",".14816828"],[1685576820000,"36444000","36438000","36444000","36430000",".16262743"],[1685576880000,"36425000","36442000","36444000","36425000",".4411"]]}'
bithumb_3m = '{"status":"0000","data":[[1685396880000,"36942000","36921000","36947000","36921000","1.89727081"],[1685397060000,"36923000","36914000","36928000","36911000",".5763"],[1685397240000,"36914000","36908000","36920000","36908000",".6438"],[1685397420000,"36914000","36909000","36914000","36906000","2.43995857"]]}'
bithumb_5m = '{"status":"0000","data":[[1684767000000,"35998000","35991000","36006000","35981000","2.88074613"],[1684767300000,"36003000","36033000","36033000","35981000","1.2032"],[1684767600000,"36038000","36004000","36089000","36001000","2.4913"],[1684767900000,"36000000","35934000","36000000","35934000","2.52842737"]]}'
bithumb_10m = '{"status":"0000","data":[[1683867000000,"35956000","35897000","35976000","35892000","20.09987236"],[1683867600000,"35896000","35834000","35904000","35831000","13.50485346"],[1683868200000,"35834000","35807000","35852000","35800000","16.49747843"]]}'
bithumb_1h = '{"status":"0000","data":[[1667599200000,"29438000","29425000","29481000","29400000","52.39353122"],[1667602800000,"29424000","29393000","29459000","29373000","45.00169619"],[1667606400000,"29392000","29834000","29837000","29333000","183.89473364"]]}'
bithumb_6h = '{"status":"0000","data":[[1577631600000,"8510000","8496000","8634000","8461000","716.77220413"],[1577653200000,"8496000","8451000","8540000","8420000","433.28495444"],[1577674800000,"8452000","8474000","8498000","8434000","521.77044521"],[1577696400000,"8472000","8402000","8487000","8391000","607.23104716"]]}'
bithumb_12h = '{"status":"0000","data":[[1469674800000,"755000","755000","757000","751000","1393.18789772"],[1469718000000,"754000","751000","755000","750000","633.63842646"],[1469761200000,"752000","750000","753000","747000","1522.66945909"],[1469804400000,"751000","749000","753000","748000","610.03369078"]]}'


@pytest.fixture(scope='function')
def mocked_requests_get(request):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

    return MockResponse(json.loads(request.param), 200)


@pytest.mark.parametrize('mocked_requests_get', [bithumb_1m, bithumb_3m, bithumb_5m, bithumb_10m, bithumb_1h, bithumb_12h], indirect=True)
@mock.patch('requests.get')
def test_bithumb_repo_without_paramemters(mock_response_get, mocked_requests_get):
    mock_response_get.return_value = mocked_requests_get
    bithumb_repo = BithumbRepo()
    response = bithumb_repo.get(filters={})
    assert isinstance(response, StockData)
    # assert isinstance(response.data.index, pd.DatetimeIndex)
    assert list(response.data.columns) == [
        'open', 'high', 'low', 'close', 'volume']
