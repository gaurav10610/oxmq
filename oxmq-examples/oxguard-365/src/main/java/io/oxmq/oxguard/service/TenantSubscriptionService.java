package io.oxmq.oxguard.service;

import io.oxmq.oxguard.connector.GraphClient;
import io.oxmq.oxguard.model.TenantProfile;
import io.oxmq.oxguard.model.TenantTier;
import jakarta.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Dynamic Multi-Tenant Subscription and Capacity Service.
 * Eliminates static tenant-to-tier mappings by dynamically querying Microsoft Graph APIs
 * (/v1.0/subscribedSkus and /v1.0/organization) to discover actual license SKUs, paid seat counts,
 * and organization metadata for all onboarded enterprise tenants.
 */
@Service
public class TenantSubscriptionService {

    private static final Logger log = LoggerFactory.getLogger(TenantSubscriptionService.class);

    private final GraphClient graphClient;
    private final List<String> onboardedTenantIds;
    private final Map<String, TenantProfile> tenantRegistry = new ConcurrentHashMap<>();

    public TenantSubscriptionService(
            GraphClient graphClient,
            @Value("${oxguard.tenants.onboarded:tenant-smb-250,tenant-mid-2500,tenant-ent-10k,tenant-large-35k,tenant-global-100k}") List<String> onboardedTenantIds
    ) {
        this.graphClient = graphClient;
        this.onboardedTenantIds = onboardedTenantIds != null ? onboardedTenantIds : List.of();
    }

    @PostConstruct
    public void init() {
        log.info("Initializing dynamic Microsoft 365 Tenant Subscription Registry for onboarded tenants: {}", onboardedTenantIds);
        for (String tenantId : onboardedTenantIds) {
            try {
                discoverAndRegister(tenantId);
            } catch (Exception e) {
                log.warn("Initial discovery deferred for tenant [{}] (will discover lazily): {}", tenantId, e.getMessage());
            }
        }
    }

    /**
     * Discovers tenant licensing, SKU, and seat counts dynamically from Microsoft Graph
     * and updates the in-memory registry.
     */
    public TenantProfile discoverAndRegister(String tenantId) {
        if (tenantId == null || tenantId.isBlank()) {
            tenantId = "tenant-mid-2500";
        }
        try {
            TenantProfile profile = graphClient.discoverTenantProfile(tenantId);
            if (profile != null) {
                tenantRegistry.put(tenantId, profile);
                log.info("Registered dynamic tenant subscription for [{}] -> Tier: {}, Seats: {}, Burst Limit: {} RUs/s",
                        tenantId, profile.tier().name(), profile.seatCount(), profile.burstLimitRUsPerSec());
                return profile;
            }
        } catch (Exception e) {
            log.warn("Could not query Microsoft Graph for tenant [{}]: {}. Applying dynamic fallback profile.", tenantId, e.getMessage());
        }

        // Resilient fallback profile if Graph API is temporarily unreachable
        return tenantRegistry.computeIfAbsent(tenantId, id -> {
            int defaultSeats = 2500;
            if (id.contains("smb") || id.contains("250")) defaultSeats = 250;
            else if (id.contains("ent") || id.contains("10k")) defaultSeats = 10000;
            else if (id.contains("large") || id.contains("35k")) defaultSeats = 35000;
            else if (id.contains("global") || id.contains("100k")) defaultSeats = 100000;
            TenantTier tier = TenantTier.fromSeatCount(defaultSeats);
            return new TenantProfile(id, "Organization (" + id + ")", tier, defaultSeats, tier.getDailyRuBudget(), tier.getDefaultBurstLimitRUs());
        });
    }

    /**
     * Resolves tenant profile dynamically.
     */
    public TenantProfile getTenantProfile(String tenantId) {
        if (tenantId == null || tenantId.isBlank()) {
            tenantId = "tenant-mid-2500";
        }
        TenantProfile profile = tenantRegistry.get(tenantId);
        if (profile == null) {
            profile = discoverAndRegister(tenantId);
        }
        return profile;
    }

    /**
     * Resolves the dynamic token bucket burst limit (RUs/s) for a given tenant.
     */
    public int getTenantBurstLimit(String tenantId) {
        return getTenantProfile(tenantId).burstLimitRUsPerSec();
    }

    /**
     * Returns all dynamically registered tenant profiles.
     */
    public List<TenantProfile> getAllTenants() {
        for (String id : onboardedTenantIds) {
            if (!tenantRegistry.containsKey(id)) {
                discoverAndRegister(id);
            }
        }
        return new ArrayList<>(tenantRegistry.values());
    }

    /**
     * Returns all onboarded tenant identifiers.
     */
    public List<String> getAllTenantIds() {
        return new ArrayList<>(onboardedTenantIds);
    }

    /**
     * Refreshes all active tenant subscriptions from Microsoft Graph.
     */
    public void refreshAllTenants() {
        for (String tenantId : onboardedTenantIds) {
            try {
                TenantProfile profile = graphClient.discoverTenantProfile(tenantId);
                if (profile != null) {
                    tenantRegistry.put(tenantId, profile);
                }
            } catch (Exception e) {
                log.warn("Error refreshing tenant [{}]: {}", tenantId, e.getMessage());
            }
        }
    }
}
