package org.br.mineradora.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.br.mineradora.client.CurrencyPriceClient;
import org.br.mineradora.dto.CurrencyPriceDTO;
import org.br.mineradora.dto.QuotationDTO;
import org.br.mineradora.entity.QuotationEntity;
import org.br.mineradora.message.KafkaEvents;
import org.br.mineradora.repository.QuotationRepository;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;

@ApplicationScoped
public class QuotationService {

    @Inject
    @RestClient
    CurrencyPriceClient currencyPriceClient;

    @Inject
    QuotationRepository quotationRepository;

    @Inject
    KafkaEvents kafkaEvents;

    private final static String PAIR = "USD-BRL";

    public void getCurrencyPrice() {

        CurrencyPriceDTO currencyPriceDTO = currencyPriceClient.getPriceByPair("USD-BR");

        if (updateCurrentInfoPrice(currencyPriceDTO)) {
            kafkaEvents.sendNewKafkaEvent(QuotationDTO
                    .builder()
                    .currencyPrice(new BigDecimal(currencyPriceDTO.getUsdbrl().getBid()))
                    .date(LocalDate.now())
                    .build());
        }


    }

    private boolean updateCurrentInfoPrice(CurrencyPriceDTO currencyPriceDTO) {

        BigDecimal currentPrice = new BigDecimal(currencyPriceDTO.getUsdbrl().getBid());
        Boolean updatePrice = false;

        List<QuotationEntity> quotationList = quotationRepository.findAll().list();

        if (quotationList.isEmpty()) {

            saveQuotation(currencyPriceDTO);
            updatePrice = true;
        } else {

            QuotationEntity lastDollarPrice = quotationList.get(quotationList.size() - 1);

            if (currentPrice.floatValue() > lastDollarPrice.getCurrencyPrice().floatValue()) {
                updatePrice = true;
                saveQuotation(currencyPriceDTO);
            }
        }

        return updatePrice;

    }

    private void saveQuotation(CurrencyPriceDTO currencyPriceDTO) {

        QuotationEntity quotation = new QuotationEntity();

        quotation.setDate(LocalDate.now());
        quotation.setCurrencyPrice(new BigDecimal(currencyPriceDTO.getUsdbrl().getBid()));
        quotation.setPctChange(currencyPriceDTO.getUsdbrl().getPctChange());
        quotation.setPair(PAIR);

        quotationRepository.persist(quotation);
    }
}
