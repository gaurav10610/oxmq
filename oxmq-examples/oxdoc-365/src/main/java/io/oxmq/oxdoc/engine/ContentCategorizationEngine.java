package io.oxmq.oxdoc.engine;

import io.oxmq.oxdoc.model.AccessScope;
import io.oxmq.oxdoc.model.ContentVisibility;
import io.oxmq.oxdoc.model.PermissionEntry;
import java.util.List;
import org.springframework.stereotype.Component;

/**
 * Pillar 1: Evaluates true effective permissions and assigns content visibility scope.
 * Identifies direct vs inherited permissions, sharing link scopes, and knowledge audience.
 */
@Component
public class ContentCategorizationEngine {

    public ContentVisibility evaluate(List<PermissionEntry> permissions) {
        if (permissions == null || permissions.isEmpty()) {
            return new ContentVisibility(AccessScope.PRIVATE, false, List.of());
        }

        AccessScope scope = AccessScope.PRIVATE;
        boolean hasUniquePermissions = false;

        for (PermissionEntry p : permissions) {
            if (!p.isInherited()) {
                hasUniquePermissions = true;
            }

            if ("anonymous".equalsIgnoreCase(p.linkScope())) {
                scope = AccessScope.PUBLIC_ANONYMOUS;
                break; // Highest exposure level reached
            } else if ("users".equalsIgnoreCase(p.linkScope())) {
                if (scope != AccessScope.PUBLIC_ANONYMOUS) {
                    scope = AccessScope.EXTERNAL_GUEST;
                }
            } else if ("organization".equalsIgnoreCase(p.linkScope())) {
                if (scope != AccessScope.PUBLIC_ANONYMOUS && scope != AccessScope.EXTERNAL_GUEST) {
                    scope = AccessScope.ORGANIZATION_WIDE;
                }
            } else if (p.grantee() != null && !p.grantee().isBlank()) {
                if (scope == AccessScope.PRIVATE && !"owner".equalsIgnoreCase(p.role())) {
                    scope = AccessScope.INTERNAL_RESTRICTED;
                }
            }
        }

        return new ContentVisibility(scope, hasUniquePermissions, permissions);
    }
}
