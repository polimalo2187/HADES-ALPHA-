import React, { useState, useEffect } from 'react';
import {
  View,
  Text,
  StyleSheet,
  TextInput,
  TouchableOpacity,
  KeyboardAvoidingView,
  Platform,
  ScrollView,
  ActivityIndicator,
  Alert,
} from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';
import { THEME } from '@config/env';
import GlassCard from '@components/GlassCard';
import BiometricButton from '@components/BiometricButton';
import { api } from '@services/api';
import biometricService from '@services/biometrics';
import wsService from '@services/websocket';
import pushNotificationService from '@services/notifications';
import type { NavigationProps } from '@navigation/types';

export const LoginScreen: React.FC<NavigationProps<'Login'>> = ({ navigation }) => {
  const [phone, setPhone] = useState('');
  const [password, setPassword] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [biometricAvailable, setBiometricAvailable] = useState(false);

  useEffect(() => {
    checkBiometric();
    initializeServices();
  }, []);

  const checkBiometric = async () => {
    const available = await biometricService.checkAvailability();
    setBiometricAvailable(available);
  };

  const initializeServices = async () => {
    await pushNotificationService.initialize();
  };

  const handleLogin = async () => {
    if (!phone || !password) {
      Alert.alert('Error', 'Por favor ingresa teléfono y contraseña');
      return;
    }

    setIsLoading(true);

    try {
      const response = await api.login(phone, password);
      
      // Conectar WebSocket
      await wsService.connect(response.tokens.accessToken);
      
      // Registrar token de notificaciones
      const pushToken = await pushNotificationService.getPushToken();
      if (pushToken) {
        await pushNotificationService.registerTokenWithBackend(pushToken);
      }

      // Navegar al dashboard
      navigation.replace('Dashboard');
    } catch (error: any) {
      Alert.alert(
        'Error de inicio de sesión',
        error.response?.data?.message || 'Credenciales inválidas'
      );
    } finally {
      setIsLoading(false);
    }
  };

  const handleBiometricAuth = async () => {
    const result = await biometricService.authenticate('Iniciar sesión en HADES');
    
    if (result.success) {
      handleLogin();
    } else {
      Alert.alert('Autenticación fallida', result.error);
    }
  };

  const formatPhone = (value: string) => {
    const numbers = value.replace(/\D/g, '');
    if (numbers.length <= 10) {
      return numbers;
    }
    return numbers.slice(0, 10);
  };

  return (
    <SafeAreaView style={styles.container}>
      <KeyboardAvoidingView
        behavior={Platform.OS === 'ios' ? 'padding' : 'height'}
        style={styles.keyboardView}
      >
        <ScrollView
          contentContainerStyle={styles.scrollContent}
          keyboardShouldPersistTaps="handled"
        >
          <View style={styles.header}>
            <Text style={styles.logo}>🎯</Text>
            <Text style={styles.title}>HADES</Text>
            <Text style={styles.subtitle}>Trading Intelligence</Text>
          </View>

          <GlassCard glow border intensity={70} style={styles.formCard}>
            <Text style={styles.formTitle}>Iniciar Sesión</Text>

            <View style={styles.inputContainer}>
              <Text style={styles.label}>Teléfono</Text>
              <TextInput
                style={styles.input}
                placeholder="+57 300 123 4567"
                placeholderTextColor={THEME.colors.textMuted}
                value={phone}
                onChangeText={(text) => setPhone(formatPhone(text))}
                keyboardType="phone-pad"
                autoCapitalize="none"
                editable={!isLoading}
              />
            </View>

            <View style={styles.inputContainer}>
              <Text style={styles.label}>Contraseña</Text>
              <TextInput
                style={styles.input}
                placeholder="••••••••"
                placeholderTextColor={THEME.colors.textMuted}
                value={password}
                onChangeText={setPassword}
                secureTextEntry
                autoCapitalize="none"
                editable={!isLoading}
              />
            </View>

            <TouchableOpacity
              style={styles.forgotPassword}
              onPress={() => {}}
              disabled={isLoading}
            >
              <Text style={styles.forgotPasswordText}>¿Olvidaste tu contraseña?</Text>
            </TouchableOpacity>

            <TouchableOpacity
              style={[
                styles.button,
                isLoading && styles.buttonDisabled,
              ]}
              onPress={handleLogin}
              disabled={isLoading}
              activeOpacity={0.7}
            >
              {isLoading ? (
                <ActivityIndicator color={THEME.colors.background} />
              ) : (
                <Text style={styles.buttonText}>INGRESAR</Text>
              )}
            </TouchableOpacity>

            {biometricAvailable && (
              <BiometricButton
                onAuthenticate={handleBiometricAuth}
                label="Ingresar"
                disabled={isLoading}
              />
            )}

            <View style={styles.divider}>
              <View style={styles.dividerLine} />
              <Text style={styles.dividerText}>o</Text>
              <View style={styles.dividerLine} />
            </View>

            <TouchableOpacity
              style={styles.registerButton}
              onPress={() => navigation.navigate('Register')}
              disabled={isLoading}
            >
              <Text style={styles.registerButtonText}>
                ¿No tienes cuenta? <Text style={styles.registerLink}>Regístrate</Text>
              </Text>
            </TouchableOpacity>
          </GlassCard>

          <View style={styles.footer}>
            <Text style={styles.footerText}>
              Al continuar aceptas nuestros{' '}
              <Text style={styles.footerLink}>Términos</Text> y{' '}
              <Text style={styles.footerLink}>Política de Privacidad</Text>
            </Text>
          </View>
        </ScrollView>
      </KeyboardAvoidingView>
    </SafeAreaView>
  );
};

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: THEME.colors.background,
  },
  keyboardView: {
    flex: 1,
  },
  scrollContent: {
    flexGrow: 1,
    justifyContent: 'center',
    padding: THEME.spacing.lg,
  },
  header: {
    alignItems: 'center',
    marginBottom: THEME.spacing.xxl,
  },
  logo: {
    fontSize: 64,
    marginBottom: THEME.spacing.sm,
  },
  title: {
    fontSize: 36,
    fontWeight: 'bold',
    color: THEME.colors.text,
    letterSpacing: 4,
  },
  subtitle: {
    fontSize: THEME.fontSize.md,
    color: THEME.colors.textSecondary,
    marginTop: THEME.spacing.xs,
  },
  formCard: {
    marginBottom: THEME.spacing.lg,
  },
  formTitle: {
    fontSize: THEME.fontSize.xl,
    fontWeight: 'bold',
    color: THEME.colors.text,
    marginBottom: THEME.spacing.lg,
    textAlign: 'center',
  },
  inputContainer: {
    marginBottom: THEME.spacing.md,
  },
  label: {
    color: THEME.colors.textSecondary,
    fontSize: THEME.fontSize.sm,
    marginBottom: THEME.spacing.xs,
    fontWeight: '600',
  },
  input: {
    backgroundColor: THEME.colors.surfaceLight,
    borderRadius: THEME.borderRadius.md,
    padding: THEME.spacing.md,
    color: THEME.colors.text,
    fontSize: THEME.fontSize.md,
    borderWidth: 1,
    borderColor: THEME.colors.border,
  },
  forgotPassword: {
    alignSelf: 'flex-end',
    marginBottom: THEME.spacing.md,
  },
  forgotPasswordText: {
    color: THEME.colors.primary,
    fontSize: THEME.fontSize.sm,
  },
  button: {
    backgroundColor: THEME.colors.primary,
    borderRadius: THEME.borderRadius.md,
    padding: THEME.spacing.md,
    alignItems: 'center',
    marginBottom: THEME.spacing.md,
  },
  buttonDisabled: {
    opacity: 0.6,
  },
  buttonText: {
    color: THEME.colors.text,
    fontSize: THEME.fontSize.md,
    fontWeight: 'bold',
    letterSpacing: 1,
  },
  divider: {
    flexDirection: 'row',
    alignItems: 'center',
    marginVertical: THEME.spacing.md,
  },
  dividerLine: {
    flex: 1,
    height: 1,
    backgroundColor: THEME.colors.border,
  },
  dividerText: {
    color: THEME.colors.textMuted,
    marginHorizontal: THEME.spacing.md,
    fontSize: THEME.fontSize.sm,
  },
  registerButton: {
    alignItems: 'center',
    paddingVertical: THEME.spacing.sm,
  },
  registerButtonText: {
    color: THEME.colors.textSecondary,
    fontSize: THEME.fontSize.sm,
  },
  registerLink: {
    color: THEME.colors.primary,
    fontWeight: 'bold',
  },
  footer: {
    alignItems: 'center',
    paddingHorizontal: THEME.spacing.md,
  },
  footerText: {
    color: THEME.colors.textMuted,
    fontSize: THEME.fontSize.xs,
    textAlign: 'center',
  },
  footerLink: {
    color: THEME.colors.primary,
    textDecorationLine: 'underline',
  },
});

export default LoginScreen;
