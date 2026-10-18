package io.oxmq.oxguard.engine;

import io.oxmq.oxguard.model.EffectiveExposure;
import io.oxmq.oxguard.model.ExposureLevel;
import io.oxmq.oxguard.model.PermissionEntry;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("ExposureEngine: Effective Permission & ACL Evaluation")
class ExposureEngineTest {

    private ExposureEngine exposureEngine;

    @BeforeEach
    void setUp() {
        exposureEngine = new ExposureEngine();
    }

    @Test
    @DisplayName("Empty or null permissions resolve to PRIVATE")
    void testEmptyPermissions() {
        EffectiveExposure res = exposureEngine.evaluate(List.of());
        assertThat(res.level()).isEqualTo(ExposureLevel.PRIVATE);
        assertThat(res.hasUniquePermissions()).isFalse();
    }

    @Test
    @DisplayName("Anonymous sharing link resolves to PUBLIC_ANONYMOUS")
    void testAnonymousSharingLink() {
        List<PermissionEntry> perms = List.of(
                new PermissionEntry("p1", "owner@contoso.com", "owner", false, null, null),
                new PermissionEntry("p2", null, "read", false, null, "anonymous")
        );

        EffectiveExposure res = exposureEngine.evaluate(perms);
        assertThat(res.level()).isEqualTo(ExposureLevel.PUBLIC_ANONYMOUS);
        assertThat(res.hasUniquePermissions()).isTrue();
    }

    @Test
    @DisplayName("External user sharing link resolves to EXTERNAL_GUEST")
    void testExternalGuestLink() {
        List<PermissionEntry> perms = List.of(
                new PermissionEntry("p1", "owner@contoso.com", "owner", false, null, null),
                new PermissionEntry("p2", "external-vendor@partner.com", "read", false, null, "users")
        );

        EffectiveExposure res = exposureEngine.evaluate(perms);
        assertThat(res.level()).isEqualTo(ExposureLevel.EXTERNAL_GUEST);
        assertThat(res.hasUniquePermissions()).isTrue();
    }

    @Test
    @DisplayName("Tenant-wide sharing link resolves to ORGANIZATION_WIDE")
    void testOrganizationWideLink() {
        List<PermissionEntry> perms = List.of(
                new PermissionEntry("p1", "owner@contoso.com", "owner", false, null, null),
                new PermissionEntry("p2", "All Authenticated Users", "read", false, null, "organization")
        );

        EffectiveExposure res = exposureEngine.evaluate(perms);
        assertThat(res.level()).isEqualTo(ExposureLevel.ORGANIZATION_WIDE);
    }

    @Test
    @DisplayName("Inherited permissions correctly track hasUniquePermissions=false")
    void testInheritedPermissions() {
        List<PermissionEntry> perms = List.of(
                new PermissionEntry("p1", "owner@contoso.com", "owner", true, "folder-root-id", null),
                new PermissionEntry("p2", "finance-team@contoso.com", "read", true, "folder-root-id", null)
        );

        EffectiveExposure res = exposureEngine.evaluate(perms);
        assertThat(res.level()).isEqualTo(ExposureLevel.INTERNAL_RESTRICTED);
        assertThat(res.hasUniquePermissions()).isFalse();
    }
}
