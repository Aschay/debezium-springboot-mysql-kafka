package com.aschay.debeziumDemo.service;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.aschay.debeziumDemo.model.Customer;
import com.aschay.debeziumDemo.repository.CDCrepository;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.data.Envelope.Operation;
import jakarta.transaction.Transactional;

@Service
public class CDCservice {

	@Autowired
	CDCrepository cdcrepo;
    @Transactional
	public void replicateData(Map<String, Object> customerData, Operation operation) {
		ObjectMapper mapper = new ObjectMapper();
		Customer customer = mapper.convertValue(customerData, Customer.class);

		if (Operation.DELETE == operation) {
			cdcrepo.deleteById(customer.getId());
		} else {
			cdcrepo.save(customer);
		}
	}

}
