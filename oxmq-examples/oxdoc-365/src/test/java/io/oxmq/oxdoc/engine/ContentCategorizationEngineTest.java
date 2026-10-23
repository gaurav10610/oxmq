package io.oxmq.oxdoc.engine;

import io.oxmq.oxdoc.model.AccessScope;
import io.oxmq.oxdoc.model.ContentVisibility;
import io.oxmq.oxdoc.model.PermissionEntry;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("ContentCategorizationEngine: Effective Permission & Knowledge Scope Evaluation")
class ContentCategorizationEngineTest {

    private ContentCategorizationEngine categorizationEngine;

    @BeforeEach
    void setUp() {
        categorizationEngine = new ContentCategorizationEngine();
    }

    @Test
    @DisplayName("Empty or null permissions resolve to PRIVATE")
    void testEmptyPermissions() {
        ContentVisibility res = categorizationEngine.evaluate(List.of());
        assertThat(res.scope()).isEqualTo(AccessScope.PRIVATE);
        assertThat(res.hasUniquePermissions()).isFalse();
    }

    @Test
    @DisplayName("Anonymous sharing link resolves to PUBLIC_ANONYMOUS")
    void testAnonymousSharingLink() {
        List<PermissionEntry> perms = List.of(
                new PermissionEntry("p1", "owner@contoso.com", "owner", false, null, null),
                new PermissionEntry("p2", null, "read", false, null, "anonymous")
        );

        ContentVisibility res = categorizationEngine.evaluate(perms);
        assertThat(res.scope()).isEqualTo(AccessScope.PUBLIC_ANONYMOUS);
        assertThat(res.hasUniquePermissions()).isTrue();
    }

    @Test
    @DisplayName("External user sharing link resolves to EXTERNAL_GUEST")
    void testExternalGuestLink() {
        List<PermissionEntry> perms = List.of(
                new PermissionEntry("p1", "owner@contoso.com", "owner", false, null, null),
                new PermissionEntry("p2", "external-vendor@partner.com", "read", false, null, "users")
        );

        ContentVisibility res = categorizationEngine.evaluate(perms);
        assertThat(res.scope()).isEqualTo(AccessScope.EXTERNAL_GUEST);
        assertThat(res.hasUniquePermissions()).isTrue();
    }

    @Test
    @DisplayName("Tenant-wide sharing link resolves to ORGANIZATION_WIDE")
    void testOrganizationWideLink() {
        List<PermissionEntry> perms = List.of(
                new PermissionEntry("p1", "owner@contoso.com", "owner", false, null, null),
                new PermissionEntry("p2", "All Authenticated Users", "read", false, null, "organization")
        );

        ContentVisibility res = categorizationEngine.evaluate(perms);
        assertThat(res.scope()).isEqualTo(AccessScope.ORGANIZATION_WIDE);
    }

    @Test
    @DisplayName("Inherited permissions correctly track hasUniquePermissions=false")
    void testInheritedPermissions() {
        List<PermissionEntry> perms = List.of(
                new PermissionEntry("p1", "owner@contoso.com", "owner", true, "folder-root-id", null),
                new PermissionEntry("p2", "finance-team@contoso.com", "read", true, "folder-root-id", null)
        );

        ContentVisibility res = categorizationEngine.evaluate(perms);
        assertThat(res.scope()).isEqualTo(AccessScope.INTERNAL_RESTRICTED);
        assertThat(res.hasUniquePermissions()).isFalse();
    }
}
