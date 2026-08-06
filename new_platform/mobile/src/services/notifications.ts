import * as Notifications from 'expo-notifications';
import * as Device from 'expo-device';
import { Platform } from 'react-native';
import { NOTIFICATIONS } from '@config/env';
import type { Notification as AppNotification } from '@types/index';

Notifications.setNotificationHandler({
  handleNotificationReceivedAsync: async (notification) => {
    console.log('[Push] Notification received:', notification);
    return {
      shouldShowAlert: NOTIFICATIONS.ENABLED,
      shouldPlaySound: NOTIFICATIONS.SOUND_ENABLED,
      shouldSetBadge: NOTIFICATIONS.BADGE_ENABLED,
    };
  },
  handleNotificationClickedAsync: async (notification) => {
    console.log('[Push] Notification clicked:', notification);
    // Aquí se puede navegar a la pantalla correspondiente
  },
});

class PushNotificationService {
  private pushToken: string | null = null;
  private isInitialized = false;

  async initialize(): Promise<void> {
    if (this.isInitialized) {
      return;
    }

    if (!Device.isDevice) {
      console.log('[Push] Push notifications only work on physical devices');
      this.isInitialized = true;
      return;
    }

    try {
      // Request permissions
      const { status: existingStatus } = await Notifications.getPermissionsAsync();
      let finalStatus = existingStatus;

      if (existingStatus !== 'granted') {
        const { status } = await Notifications.requestPermissionsAsync();
        finalStatus = status;
      }

      if (finalStatus !== 'granted') {
        console.log('[Push] Permission not granted');
        this.isInitialized = true;
        return;
      }

      // Get push token
      if (Platform.OS === 'android') {
        await Notifications.setNotificationChannelAsync('default', {
          name: 'default',
          importance: Notifications.AndroidImportance.MAX,
          vibrationPattern: [0, 250, 250, 250],
          lightColor: '#7c3aed',
          sound: 'notification-sound.wav',
        });
      }

      const tokenData = await Notifications.getExpoPushTokenAsync({
        projectId: process.env.EXPO_PUBLIC_PROJECT_ID,
      });

      this.pushToken = tokenData.data;
      console.log('[Push] Token:', this.pushToken);

      // Configure Android icon
      if (Platform.OS === 'android') {
        Notifications.setNotificationCategoryAsync('signal', {
          actions: [],
          intentFilters: [
            {
              action: 'SIGNAL_ALERT',
            },
          ],
        });
      }

      this.isInitialized = true;
    } catch (error) {
      console.error('[Push] Error initializing:', error);
      this.isInitialized = true;
    }
  }

  async scheduleLocalNotification(
    title: string,
    body: string,
    data?: any,
    delaySeconds?: number
  ): Promise<string> {
    if (!NOTIFICATIONS.ENABLED) {
      return '';
    }

    const notificationId = Date.now().toString();

    await Notifications.scheduleNotificationAsync({
      content: {
        title,
        body,
        data,
        sound: NOTIFICATIONS.SOUND_ENABLED ? 'notification-sound.wav' : undefined,
        priority: Notifications.AndroidNotificationPriority.HIGH,
        categoryIdentifier: 'signal',
      },
      trigger: delaySeconds
        ? {
            seconds: delaySeconds,
          }
        : null,
    });

    return notificationId;
  }

  async showImmediateNotification(
    title: string,
    body: string,
    data?: any
  ): Promise<void> {
    if (!NOTIFICATIONS.ENABLED) {
      return;
    }

    await Notifications.scheduleNotificationAsync({
      content: {
        title,
        body,
        data,
        sound: NOTIFICATIONS.SOUND_ENABLED ? 'notification-sound.wav' : undefined,
        priority: Notifications.AndroidNotificationPriority.HIGH,
        vibrate: NOTIFICATIONS.VIBRATION_ENABLED ? [0, 250, 250, 250] : undefined,
      },
      trigger: null,
    });
  }

  async cancelNotification(notificationId: string): Promise<void> {
    await Notifications.cancelScheduledNotificationAsync(notificationId);
  }

  async cancelAllNotifications(): Promise<void> {
    await Notifications.cancelAllScheduledNotificationsAsync();
    await Notifications.dismissAllNotificationsAsync();
  }

  async getPushToken(): Promise<string | null> {
    if (!this.isInitialized) {
      await this.initialize();
    }
    return this.pushToken;
  }

  async registerTokenWithBackend(token: string): Promise<void> {
    // Enviar el token al backend para notificaciones push
    // Esto se manejaría desde el servicio de API
    console.log('[Push] Registering token with backend:', token);
  }

  async unregisterTokenFromBackend(): Promise<void> {
    // Eliminar el token del backend
    console.log('[Push] Unregistering token from backend');
  }

  async getBadgeCount(): Promise<number> {
    return await Notifications.getBadgeCountAsync();
  }

  async setBadgeCount(count: number): Promise<void> {
    if (NOTIFICATIONS.BADGE_ENABLED) {
      await Notifications.setBadgeCountAsync(count);
    }
  }

  async incrementBadgeCount(incrementBy?: number): Promise<void> {
    if (NOTIFICATIONS.BADGE_ENABLED) {
      const current = await this.getBadgeCount();
      await this.setBadgeCount(current + (incrementBy || 1));
    }
  }

  async clearBadgeCount(): Promise<void> {
    await this.setBadgeCount(0);
  }
}

export const pushNotificationService = new PushNotificationService();
export default pushNotificationService;
