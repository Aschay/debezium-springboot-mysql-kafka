package com.aschay.debeziumEmbedded.repository;

import java.util.Optional;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.aschay.debeziumEmbedded.model.Customer;

@Repository
public interface CDCrepository extends JpaRepository<Customer, UUID> {

	Optional<Customer> findById(UUID id);

	boolean existsById(UUID id);

}
