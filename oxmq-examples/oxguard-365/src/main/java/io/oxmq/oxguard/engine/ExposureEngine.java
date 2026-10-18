package io.oxmq.oxguard.engine;

import io.oxmq.oxguard.model.EffectiveExposure;
import io.oxmq.oxguard.model.ExposureLevel;
import io.oxmq.oxguard.model.PermissionEntry;
import java.util.List;
import org.springframework.stereotype.Component;

/**
 * Pillar 1: Evaluates true effective permissions and assigns exposure classification.
 * Identifies direct vs inherited permissions and sharing link scopes.
 */
@Component
public class ExposureEngine {

    public EffectiveExposure evaluate(List<PermissionEntry> permissions) {
        if (permissions == null || permissions.isEmpty()) {
            return new EffectiveExposure(ExposureLevel.PRIVATE, false, List.of());
        }

        ExposureLevel exposure = ExposureLevel.PRIVATE;
        boolean hasUniquePermissions = false;

        for (PermissionEntry p : permissions) {
            if (!p.isInherited()) {
                hasUniquePermissions = true;
            }

            if ("anonymous".equalsIgnoreCase(p.linkScope())) {
                exposure = ExposureLevel.PUBLIC_ANONYMOUS;
                break; // Highest exposure level reached
            } else if ("users".equalsIgnoreCase(p.linkScope())) {
                if (exposure != ExposureLevel.PUBLIC_ANONYMOUS) {
                    exposure = ExposureLevel.EXTERNAL_GUEST;
                }
            } else if ("organization".equalsIgnoreCase(p.linkScope())) {
                if (exposure != ExposureLevel.PUBLIC_ANONYMOUS && exposure != ExposureLevel.EXTERNAL_GUEST) {
                    exposure = ExposureLevel.ORGANIZATION_WIDE;
                }
            } else if (p.grantee() != null && !p.grantee().isBlank()) {
                if (exposure == ExposureLevel.PRIVATE && !"owner".equalsIgnoreCase(p.role())) {
                    exposure = ExposureLevel.INTERNAL_RESTRICTED;
                }
            }
        }

        return new EffectiveExposure(exposure, hasUniquePermissions, permissions);
    }
}
