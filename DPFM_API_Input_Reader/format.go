package dpfm_api_input_reader

import (
	"data-platform-api-partner-function-exconf-rmq-kube/DPFM_API_Caller/requests"
)

func (sdc *SDC) ConvertToPartnerFunction() *requests.PartnerFunction {
	data := sdc.PartnerFunction
	return &requests.PartnerFunction{
		PartnerFunction: data.PartnerFunction,
	}
}
