import React, { useState, useEffect } from 'react';
import { View, Text, StyleSheet, TouchableOpacity, ActivityIndicator } from 'react-native';
import * as LocalAuthentication from 'expo-local-authentication';
import { THEME } from '@config/env';
import GlassCard from './GlassCard';

interface BiometricButtonProps {
  onAuthenticate: () => void;
  label?: string;
  disabled?: boolean;
}

export const BiometricButton: React.FC<BiometricButtonProps> = ({
  onAuthenticate,
  label = 'Autenticar',
  disabled = false,
}) => {
  const [isAvailable, setIsAvailable] = useState(false);
  const [biometryType, setBiometryType] = useState<string>('');
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    checkBiometricAvailability();
  }, []);

  const checkBiometricAvailability = async () => {
    try {
      const hasHardware = await LocalAuthentication.hasHardwareAsync();
      const isEnrolled = await LocalAuthentication.isEnrolledAsync();
      const supportedTypes = await LocalAuthentication.supportedAuthenticationTypesAsync();

      setIsAvailable(hasHardware && isEnrolled);

      if (supportedTypes.includes(LocalAuthentication.AuthenticationType.FACIAL_RECOGNITION)) {
        setBiometryType('Face ID');
      } else if (supportedTypes.includes(LocalAuthentication.AuthenticationType.FINGERPRINT)) {
        setBiometryType('Huella');
      } else {
        setBiometryType('Biometría');
      }
    } catch (error) {
      console.error('Error checking biometric availability:', error);
      setIsAvailable(false);
    }
  };

  const handleAuthenticate = async () => {
    if (!isAvailable || disabled) {
      return;
    }

    setIsLoading(true);

    try {
      const result = await LocalAuthentication.authenticateAsync({
        promptMessage: 'Autenticar para continuar',
        fallbackLabel: 'Usar contraseña',
        cancelLabel: 'Cancelar',
        disableDeviceFallback: true,
      });

      if (result.success) {
        onAuthenticate();
      }
    } catch (error) {
      console.error('Authentication error:', error);
    } finally {
      setIsLoading(false);
    }
  };

  if (!isAvailable) {
    return null;
  }

  return (
    <TouchableOpacity
      onPress={handleAuthenticate}
      disabled={disabled || isLoading}
      activeOpacity={0.7}
    >
      <GlassCard glow border style={styles.container}>
        <View style={styles.content}>
          {isLoading ? (
            <ActivityIndicator color={THEME.colors.primary} size="large" />
          ) : (
            <>
              <View style={styles.iconContainer}>
                {biometryType === 'Face ID' ? (
                  <Text style={styles.icon}>👤</Text>
                ) : (
                  <Text style={styles.icon}>👆</Text>
                )}
              </View>
              <Text style={styles.label}>
                {label} con {biometryType}
              </Text>
            </>
          )}
        </View>
      </GlassCard>
    </TouchableOpacity>
  );
};

const styles = StyleSheet.create({
  container: {
    marginVertical: THEME.spacing.md,
  },
  content: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'center',
    paddingVertical: THEME.spacing.md,
  },
  iconContainer: {
    marginRight: THEME.spacing.sm,
    width: 40,
    height: 40,
    borderRadius: 20,
    backgroundColor: THEME.colors.primary + '20',
    alignItems: 'center',
    justifyContent: 'center',
  },
  icon: {
    fontSize: 20,
  },
  label: {
    color: THEME.colors.text,
    fontSize: THEME.fontSize.md,
    fontWeight: '600',
  },
});

export default BiometricButton;
