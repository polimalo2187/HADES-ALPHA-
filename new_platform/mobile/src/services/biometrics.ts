import * as LocalAuthentication from 'expo-local-authentication';
import { AUTH } from '@config/env';

export interface BiometricResult {
  success: boolean;
  error?: string;
  biometryType?: BiometryType;
}

export type BiometryType = 'face' | 'fingerprint' | 'iris' | 'none';

class BiometricService {
  private isAvailableCache: boolean | null = null;
  private biometryTypeCache: BiometryType | null = null;

  async checkAvailability(): Promise<boolean> {
    if (this.isAvailableCache !== null) {
      return this.isAvailableCache;
    }

    try {
      const hasHardware = await LocalAuthentication.hasHardwareAsync();
      const isEnrolled = await LocalAuthentication.isEnrolledAsync();

      this.isAvailableCache = hasHardware && isEnrolled;
      return this.isAvailableCache;
    } catch (error) {
      console.error('[Biometric] Error checking availability:', error);
      this.isAvailableCache = false;
      return false;
    }
  }

  async getBiometryType(): Promise<BiometryType> {
    if (this.biometryTypeCache !== null) {
      return this.biometryTypeCache;
    }

    try {
      const supportedTypes = await LocalAuthentication.supportedAuthenticationTypesAsync();

      if (supportedTypes.includes(LocalAuthentication.AuthenticationType.FACIAL_RECOGNITION)) {
        this.biometryTypeCache = 'face';
      } else if (supportedTypes.includes(LocalAuthentication.AuthenticationType.FINGERPRINT)) {
        this.biometryTypeCache = 'fingerprint';
      } else if (supportedTypes.includes(LocalAuthentication.AuthenticationType.IRIS)) {
        this.biometryTypeCache = 'iris';
      } else {
        this.biometryTypeCache = 'none';
      }

      return this.biometryTypeCache;
    } catch (error) {
      console.error('[Biometric] Error getting biometry type:', error);
      this.biometryTypeCache = 'none';
      return 'none';
    }
  }

  async authenticate(promptMessage?: string): Promise<BiometricResult> {
    if (!AUTH.BIOMETRIC_ENABLED) {
      return {
        success: false,
        error: 'Biometric authentication is disabled',
      };
    }

    try {
      const isAvailable = await this.checkAvailability();
      if (!isAvailable) {
        return {
          success: false,
          error: 'Biometric authentication is not available',
        };
      }

      const biometryType = await this.getBiometryType();

      const result = await LocalAuthentication.authenticateAsync({
        promptMessage: promptMessage || 'Autenticar para continuar',
        fallbackLabel: 'Usar contraseña',
        cancelLabel: 'Cancelar',
        disableDeviceFallback: true, // Solo biométrico
      });

      if (result.success) {
        return {
          success: true,
          biometryType,
        };
      } else {
        return {
          success: false,
          error: result.error?.message || 'Autenticación fallida',
          biometryType,
        };
      }
    } catch (error: any) {
      console.error('[Biometric] Authentication error:', error);
      return {
        success: false,
        error: error.message || 'Error en autenticación biométrica',
      };
    }
  }

  async clearCache(): void {
    this.isAvailableCache = null;
    this.biometryTypeCache = null;
  }

  async getCapabilities(): Promise<{
    available: boolean;
    type: BiometryType;
    enrolled: boolean;
  }> {
    const available = await this.checkAvailability();
    const type = await this.getBiometryType();

    let enrolled = false;
    try {
      enrolled = await LocalAuthentication.isEnrolledAsync();
    } catch {
      enrolled = false;
    }

    return { available, type, enrolled };
  }
}

export const biometricService = new BiometricService();
export default biometricService;
